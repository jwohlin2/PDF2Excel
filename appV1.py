# app.py — OCR-first + bounded LLM fixer → Excel
# - Table detection (OpenCV) + cell-based OCR (Tesseract)
# - Robust label normalization (synonyms, punctuation-insensitive)
# - Bounded LLM “fixer” with grammar (no invented rows/cols)
# - Preserves statement ordering & recomputes totals
# - Robust Excel autosize (xlsxwriter or openpyxl)
# - Tkinter GUI

import os, re, json, tempfile, subprocess, traceback, io, shutil
from pathlib import Path
from typing import List, Dict, Tuple, Optional

import numpy as np
import pandas as pd
from PIL import Image
from pdf2image import convert_from_bytes
import pytesseract
import cv2
import sys
import argparse

from tkinter import Tk, filedialog, Button, Label, StringVar, messagebox
from tkinter import ttk
from datetime import datetime

# --- Optional fuzzy matching (safe fallback if not installed) ---
try:
    from thefuzz import fuzz  # pip install thefuzz[speedup]
except Exception:
    class _FuzzStub:
        @staticmethod
        def ratio(a, b): return 0
    fuzz = _FuzzStub()
# --- Qwen-VL availability check (do this AFTER LLAMA_EXE/MODEL_TXT are defined) ---
from pathlib import Path



# Grammar: a single number token like 3,410 or (5,000) or -417 (no words)
QWEN_NUM_GBNF = r"""
root     ::= _ number _
number   ::= neg? (paren | plain)
neg      ::= "-"
plain    ::= DIGIT (DIGIT | COMMA)* ("." DIGIT+)?    # 12,345.67
paren    ::= "(" _ DIGIT (DIGIT | COMMA)* ("." DIGIT+)? _ ")"   # (5,000)
COMMA    ::= %x2C
DIGIT    ::= %x30-39
_        ::= ( %x09 | %x0A | %x0D | %x20 )*
"""

# -------------------- EDIT THESE PATHS --------------------
BASE_DIR     = Path(r"D:\Pdf2ExcelOffline")
POPPLER_BIN  = BASE_DIR / "poppler-bin"
LLAMA_BIN    = BASE_DIR / "llama-bin"
LLAMA_EXE    = LLAMA_BIN / "llama-mtmd-cli.exe"          # text-only is fine
MODEL_TXT    = BASE_DIR / "models" / "qwen2.5-vl-7b-instruct-q4_k_m.gguf"

# Tesseract (Windows)
pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"

# -------------------- OCR / LLM KNOBS --------------------
OCR_CONF_THRESH   = 40
MAX_LONG_SIDE     = 1800           # slight bump for grid clarity

LLM_TIMEOUT_S     = 120
LLM_CTX           = 2048
LLM_TEMP          = "0"
LLM_TOPK          = "1"
LLM_TOPP          = "0"
LLM_NGL           = "0"            # CPU for stability
DEFAULT_THREADS   = str(os.cpu_count() or 8)

# -------------------- Statement ordering --------------------
ORDER = [
    "sales",
      "client services revenue", "client service revenue", "book sales",
      "professional consultation",
      "total sales",

    "expenses",
      "wages", "wages and benefits", "marketing and advertising", "rent",
      "utilities", "memberships and publications", "insurance", "consultants",
      "office supplies",
      "total expenses",

    "operating income",

    "non-operating gains (losses)",
      "interest income, net", "interest expense", "loss on sale of assets", "donations (gift)",
      "other income", "other expense", "other income/(expense)",
      "total non-operating gains (losses)",

    "provision for income taxes", "income tax expense",
    "net income (loss)", "net income",
    "gross profit", "cost of goods sold", "cost of revenue",
    "income before taxes",
]
ORDER_RANK = {k: i for i, k in enumerate(ORDER)}

# Canonical, synonyms, whitelist
CANONICAL = list(dict.fromkeys(ORDER))  # unique, in order
SYNONYMS = {
    "client service revenue":"client services revenue",
    "donations gift": "donations (gift)",
    "net income loss": "net income (loss)",
    "interest income net": "interest income, net",
    "interest income":"interest income, net",
    "income tax expense":"provision for income taxes",
    "loss on disposal of assets":"loss on sale of assets",
    "operating income (loss)":"operating income",
    "operating income (losses)":"operating income",
    "total operating expenses":"total expenses",
    "cogs":"cost of goods sold",
    "cost of sales":"cost of goods sold",
    "other expenses":"other expense",
    "other incomes":"other income",
    # new: common renderings for the mixed line
    "other income (expense)": "other income/(expense)",
    "other income expense": "other income/(expense)",
}
WHITELIST = set(CANONICAL) | set(SYNONYMS.values())

# Rows to keep even without numbers
HEADERS_KEEP = {"sales", "expenses", "non-operating gains (losses)"}

# Helpers to detect accidental "year rows"
YEARS_PAIR_RE = re.compile(r"^\s*(19|20)\d{2}\s+(19|20)\d{2}\s*$")

def _is_year_label(s: str) -> bool:
    s = str(s or "").strip()
    return bool(HDR_YEAR_RE.fullmatch(s)) or bool(YEARS_PAIR_RE.fullmatch(s))

# -------------------- Regex + helpers --------------------
try:
    RESAMPLE = Image.Resampling.LANCZOS
except AttributeError:
    RESAMPLE = Image.LANCZOS

YEAR         = re.compile(r"^(19|20)\d{2}$")
HDR_YEAR_RE  = re.compile(r"^(19|20)\d{2}$")
NUM = re.compile(r"^\(?-?\$?[\d,]+(?:\.\d+)?\)?(?:DR|%)?$")
DROP_PAT     = re.compile(r"(years?\s+ended|statement of|unaudited|page\s*\d+)", re.I)
# --- Sloppy numeric token that allows spaces inside () and supports DR/%, $ ---
NUM_TOKEN_SLOPPY = re.compile(r"""
    \(\s*-?\$?[\d,]+(?:\.\d+)?\s*\)   # ( 5,000 ) or (5,000)
  | -?\$?[\d,]+(?:\.\d+)?(?:DR|%)?    # 5,000 | -417 | 12.5% | 350DR
""", re.I | re.X)

def _normalize_num_token(tok: str) -> str:
    # remove internal spaces so "( 5,000 )" -> "(5,000)"
    return re.sub(r"\s+", "", tok or "")

def _is_rule_line(text: str) -> bool:
    # ignore pure underline/rule lines
    return bool(re.fullmatch(r"[_\-\u2014\=\s]+", (text or "").strip()))

def _is_numlike(s) -> bool:
    if s is None: return False
    t = str(s).strip()
    if not t: return False
    return bool(NUM.match(t))

def _to_number(s: str):
    s = str(s).strip()
    if s == "" or s == "-":
        return None
    
    # Handle percentages
    if s.endswith("%"):
        try:
            return float(s.replace("%", "").strip()) / 100
        except:
            return None
    
    # Handle numbers with commas and parentheses
    s = s.replace(",", "")
    is_negative = False
    if s.startswith("(") and s.endswith(")"):
        is_negative = True
        s = s[1:-1]
    
    try:
        val = float(s)
        return -val if is_negative else val
    except ValueError:
        return None


# --- Normalization helpers (robust against punctuation/typos) ---
def _norm_key(s: str) -> str:
    s = (s or "").lower()
    s = re.sub(r"[^a-z0-9 ]+", " ", s)     # strip punctuation, $ signs, etc.
    s = re.sub(r"\s+", " ", s).strip()
    return s

NORM_SYNONYMS = { _norm_key(k): _norm_key(v) for k, v in SYNONYMS.items() }
CANON_BY_NORM = { _norm_key(k): k for k in CANONICAL }  # normalized -> canonical spelling

def _canon_label(s: str) -> str:
    # first use your robust normalization + synonym map
    k = _norm_key(s)
    k = NORM_SYNONYMS.get(k, k)
    # exact canonical hit?
    if k in CANON_BY_NORM:
        return CANON_BY_NORM[k]
    # fuzzy fallback
    candidates = WHITELIST | set(SYNONYMS.values())
    best_name, best_score = None, -1
    for cand in candidates:
        score = fuzz.ratio(k, _norm_key(cand))
        if score > best_score:
            best_name, best_score = cand, score
    return best_name if best_score >= 90 else k

def _norm_cat_for_match(s: str) -> str:
    k = _norm_key(s)
    return NORM_SYNONYMS.get(k, k)

def order_like_statement(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["__rank"] = df["Category"].map(lambda s: ORDER_RANK.get(s, 99_999))
    if "__y" in df.columns:
        df = df.sort_values(["__rank", "__y"], ascending=[True, True])
    else:
        df = df.sort_values(["__rank", "Category"], ascending=[True, True])
    return df.drop(columns=["__rank"], errors="ignore")


TOTALS_TARGETS = {
    "total non-operating gains (losses)",
    "net income (loss)",
}



# -------------------- Totals --------------------
# --- DISABLED: totals are handled by the LLM edit script ---
def recompute_totals(df: pd.DataFrame) -> pd.DataFrame:
    """No-op: totals are set only by the LLM/auto-ops now."""
    return df

def _drop_year_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Removes any rows that look like just a year."""
    if df is None or df.empty or "Category" not in df.columns:
        return df
    
    # Identify rows where the "Category" value is a 4-digit number (a year)
    is_year_row = df["Category"].astype(str).str.match(r"^\d{4}$")
    
    return df[~is_year_row]

def _coerce_numeric_inplace(df: pd.DataFrame) -> pd.DataFrame:
    """Attempts to convert all columns (except 'Category') to numeric."""
    if df is None or df.empty:
        return df
    
    for c in df.columns:
        if c != "Category":
            # Coerce errors will turn non-numeric values into NaN
            df[c] = pd.to_numeric(df[c], errors="coerce")
            
    return df
# -------------------- Imaging --------------------
def prepare_image(img: Image.Image, max_long_side:int) -> Image.Image:
    w, h = img.size
    m = max(w, h)
    if m > max_long_side:
        r = max_long_side / m
        img = img.resize((int(w*r), int(h*r)), RESAMPLE)
    return img

def remove_underlines(img: Image.Image) -> Image.Image:
    """
    Remove long horizontal underlines to improve OCR.
    Returns a cleaned PIL image.
    """
    g = cv2.cvtColor(np.array(img), cv2.COLOR_RGB2GRAY)
    bw = cv2.adaptiveThreshold(g, 255, cv2.ADAPTIVE_THRESH_MEAN_C,
                               cv2.THRESH_BINARY_INV, 31, 15)
    klen = max(20, g.shape[1] // 35)
    horiz_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (klen, 1))
    horiz_lines = cv2.morphologyEx(bw, cv2.MORPH_OPEN, horiz_kernel, iterations=1)
    mask = cv2.threshold(horiz_lines, 0, 255, cv2.THRESH_BINARY)[1]
    cleaned = cv2.inpaint(g, mask, 3, cv2.INPAINT_TELEA)
    return Image.fromarray(cv2.cvtColor(cleaned, cv2.COLOR_GRAY2RGB))

def pdf_or_images_to_pages(paths: List[Path], dpi:int=220) -> List[Image.Image]:
    out = []
    for p in paths:
        if p.suffix.lower() == ".pdf":
            with open(p, "rb") as f:
                pdf = f.read()
            imgs = convert_from_bytes(pdf, dpi=dpi, fmt="png", poppler_path=str(POPPLER_BIN))
            out.extend(imgs)
        else:
            out.append(Image.open(p).convert("RGB"))
    return out

# -------------------- Table detection + cell OCR --------------------
def _merge_positions(vals: List[int], tol: int = 6) -> List[int]:
    """Merge nearly-equal coordinates to unique centers."""
    if not vals: return []
    vals = sorted(vals)
    merged = [vals[0]]
    for v in vals[1:]:
        if abs(v - merged[-1]) > tol:
            merged.append(v)
        else:
            merged[-1] = int((merged[-1] + v) / 2)
    return merged

def detect_and_extract_table(img: Image.Image) -> Tuple[List[List[str]], np.ndarray]:
    """
    Detects table structure using morphology and extracts cell text.
    Falls back to returning [] if grid cannot be detected (the caller now has a fallback path).
    """
    rgb = np.array(img)
    gray = cv2.cvtColor(rgb, cv2.COLOR_RGB2GRAY)

    def try_grid(binary_img: np.ndarray):
        h, w = binary_img.shape[:2]
        kx = max(1, w // 120)
        ky = max(1, h // 120)
        vert_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (1, max(10, ky * 7)))
        horz_kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (max(10, kx * 7), 1))

        vertical = cv2.erode(binary_img, vert_kernel, iterations=2)
        vertical = cv2.dilate(vertical, vert_kernel, iterations=2)

        horizontal = cv2.erode(binary_img, horz_kernel, iterations=2)
        horizontal = cv2.dilate(horizontal, horz_kernel, iterations=2)

        grid = cv2.addWeighted(vertical, 0.5, horizontal, 0.5, 0.0)
        grid = cv2.dilate(grid, np.ones((3,3), np.uint8), iterations=1)

        contours, _ = cv2.findContours(grid, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        if not contours:
            return None

        table_contour = max(contours, key=cv2.contourArea)
        x, y, w, h = cv2.boundingRect(table_contour)
        if w < 50 or h < 50:
            return None

        roi_gray = gray[y:y+h, x:x+w]
        roi_bin  = binary_img[y:y+h, x:x+w]

        # Recompute lines inside ROI
        v = cv2.erode(roi_bin, vert_kernel, iterations=1)
        v = cv2.dilate(v, vert_kernel, iterations=1)
        hmask = cv2.erode(roi_bin, horz_kernel, iterations=1)
        hmask = cv2.dilate(hmask, horz_kernel, iterations=1)

        v_conts, _ = cv2.findContours(v, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        h_conts, _ = cv2.findContours(hmask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

        def _merge_positions(vals: List[int], tol: int = 6) -> List[int]:
            if not vals: return []
            vals = sorted(vals)
            merged = [vals[0]]
            for val in vals[1:]:
                if abs(val - merged[-1]) > tol:
                    merged.append(val)
                else:
                    merged[-1] = (merged[-1] + val) // 2
            return merged

        xs = []
        for c in v_conts:
            xx, yy, ww, hh = cv2.boundingRect(c)
            if hh > roi_gray.shape[0] * 0.25:
                xs.extend([xx, xx + ww])
        xs = [p for p in xs if 0 <= p <= roi_gray.shape[1]]
        xs = _merge_positions([0, *xs, roi_gray.shape[1]], tol=8)

        ys = []
        for c in h_conts:
            xx, yy, ww, hh = cv2.boundingRect(c)
            if ww > roi_gray.shape[1] * 0.25:
                ys.extend([yy, yy + hh])
        ys = [p for p in ys if 0 <= p <= roi_gray.shape[0]]
        ys = _merge_positions([0, *ys, roi_gray.shape[0]], tol=8)

        if len(xs) < 2 or len(ys) < 2:
            return None

        # Extract grid cells
        table_data: List[List[str]] = []
        for iy in range(len(ys)-1):
            row: List[str] = []
            y1, y2 = ys[iy], ys[iy+1]
            if (y2 - y1) < 8: 
                continue
            for ix in range(len(xs)-1):
                x1, x2 = xs[ix], xs[ix+1]
                if (x2 - x1) < 8:
                    continue
                cell = roi_gray[y1:y2, x1:x2]
                cell_blur = cv2.GaussianBlur(cell, (3,3), 0)
                cell_proc = cv2.threshold(cell_blur, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)[1]
                text = pytesseract.image_to_string(cell_proc, config="--psm 6 -c preserve_interword_spaces=1").strip()
                row.append(text)
            if row and any(s.strip() for s in row):
                table_data.append(row)

        if len(table_data) >= 2 and any(len(r) >= 2 for r in table_data):
            return (table_data, gray)
        return None

    # Try multiple binarizations
    tries = []
    tries.append(cv2.adaptiveThreshold(gray, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
                                       cv2.THRESH_BINARY_INV, 15, 3))
    _, otsu_inv = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)
    tries.append(otsu_inv)
    _, otsu = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    tries.append(255 - otsu)

    for bin_img in tries:
        out = try_grid(bin_img)
        if out is not None:
            return out

    # Nothing found → caller will trigger fallback OCR-by-lines
    return [], gray

# -------------------- Parse table → DataFrame --------------------
def _infer_headers(row0: List[str]) -> List[str]:
    hdrs = []
    for i, h in enumerate(row0):
        clean = re.sub(r"[\u200b\ufeff]+", "", str(h or "")).strip()
        if i == 0:
            hdrs.append("Category")
            continue
        if HDR_YEAR_RE.fullmatch(clean):
            hdrs.append(clean)
        else:
            lc = clean.lower()
            if lc in {"ttm", "ytd"}:
                hdrs.append(clean.upper())
            else:
                hdrs.append(clean if clean else f"Col{i}")
    return hdrs

def parse_finance_table(table_data: List[List[str]]) -> pd.DataFrame:
    if not table_data:
        return pd.DataFrame()

    headers = _infer_headers(table_data[0])
    data = table_data[1:]

    norm_rows = []
    for r in data:
        if len(r) < len(headers):
            r = r + [""] * (len(headers) - len(r))
        elif len(r) > len(headers):
            r = r[:len(headers)]
        norm_rows.append(r)

    df = pd.DataFrame(norm_rows, columns=headers)

    df["Category"] = df["Category"].map(lambda s: _canon_label(s or ""))
    for c in df.columns:
        if c != "Category":
            df[c] = df[c].map(_to_number)

    val_cols = [c for c in df.columns if c != "Category"]
    if val_cols:
        df = df.dropna(subset=val_cols, how="all")
    df = df[~df["Category"].map(_is_numlike)]
    df = df.reset_index(drop=True)
    return df

def _detect_years_in_header(img: Image.Image) -> List[str]:
    arr = np.array(img)
    gray = cv2.cvtColor(arr, cv2.COLOR_RGB2GRAY)
    H, W = gray.shape[:2]
    band = gray[: max(1, int(0.30 * H)), :]
    data = pytesseract.image_to_data(band, output_type=pytesseract.Output.DATAFRAME, config="--psm 6")
    years = []
    try:
        data = data.dropna(subset=["text"])
        for _, r in data.iterrows():
            t = str(r["text"]).strip()
            if YEAR.fullmatch(t):
                years.append((int(r.get("left", 0)), t))
    except Exception:
        pass
    years = sorted({(x, y) for x, y in years}, key=lambda z: z[0])
    return [y for _, y in years]

def _rename_periods_to_years(df: pd.DataFrame, years_hint: List[str]) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    if not years_hint:
        return df

    # dedupe + keep order
    yrs = []
    for y in years_hint:
        y = str(y).strip()
        if HDR_YEAR_RE.fullmatch(y) and y not in yrs:
            yrs.append(y)

    mapping = {}
    if "Period1" in df.columns and len(yrs) >= 1:
        mapping["Period1"] = yrs[0]
    if "Period2" in df.columns and len(yrs) >= 2:
        mapping["Period2"] = yrs[1]

    if mapping:
        df = df.rename(columns=mapping)

    # After rename, if columns still duplicate (e.g. only one year found),
    # keep the first occurrence.
    df = df.loc[:, ~df.columns.duplicated()]
    return df


# -------------------- LLM fixer (bounded grammar) --------------------
JSON_GRAMMAR = r"""
root      ::= _ "{" _ "\"ops\"" _ ":" _ "[" _ ops? _ "]" _ "}" _
ops       ::= op ( _ "," _ op )*
op        ::= "{" _ "\"op\"" _ ":" _ opkind _ ( _ "," _ kv _ )* _ "}"
opkind    ::= "\"rename\"" | "\"swap_columns\"" | "\"fix_number\"" | "\"fill_missing\"" | "\"drop_row\"" | "\"calculate_total\"" | "\"add_and_calculate_row\"" | "\"derive_missing_value\""
kv        ::= key _ ":" _ val
key       ::= "\"row\"" | "\"col\"" | "\"to\"" | "\"col_a\"" | "\"col_b\"" | "\"from\"" | "\"value\"" | "\"reason\"" | "\"components\"" | "\"category\"" | "\"index\""
val       ::= number | string | array
number    ::= "-"? DIGIT+ ("." DIGIT+)? 
string    ::= "\"" char* "\""
array     ::= "[" ( string ( _ "," _ string )* )? "]"
char      ::= %x20-21 | %x23-5B | %x5D-7E
_         ::= ( %x09 | %x0A | %x0D | %x20 )*
DIGIT     ::= %x30-39
"""

def run_llm_fixer(payload: dict, tmpdir: Path) -> dict:
    # Gracefully skip if local LLM binary/model are unavailable
    try:
        if not LLAMA_BIN.exists() or not LLAMA_EXE.exists() or not MODEL_TXT.exists():
            # Return empty ops to indicate no changes; upstream will proceed without LLM fixes
            return {"ops": []}
    except Exception:
        return {"ops": []}

    gpath = tmpdir / "edit.gbnf"
    gpath.write_text(JSON_GRAMMAR.strip(), encoding="utf-8")

    prompt_sys = (
        "You are a STRICT financial table reasoner. Reconstruct a valid financial statement "
        "from the provided table. Use logic only from data present; NEVER invent rows/columns.\n"
        "Output ONLY JSON with 'ops' constrained by the grammar. Allowed ops: rename (to whitelist/synonyms), "
        "swap_columns, fix_number, fill_missing, calculate_total, add_and_calculate_row, derive_missing_value.\n"
        "**Guidance**:\n"
        "1) Use rename to map categories to the whitelist/synonyms.\n"
        "2) Use calculate_total when a 'total' is missing but components exist.\n"
        "3) Use derive_missing_value for key derived metrics when components exist "
        "(e.g., total non-operating gains (losses) = interest income, net - interest expense "
        "+ loss on sale of assets + donations (gift) + other income - other expense; "
        "net income (loss) = operating income + total non-operating gains (losses) - provision for income taxes).\n"
        "4) fix_number only when a number is clearly misread.\n"
        "Temperature must be 0; do not add new rows or columns outside these ops."
    )

    user = json.dumps(payload, ensure_ascii=False)

    cmd = [
        str(LLAMA_EXE),
        "-m", str(MODEL_TXT),
        "-t", DEFAULT_THREADS,
        "--ctx-size", str(LLM_CTX),
        "--temp", LLM_TEMP,
        "--top-k", LLM_TOPK,
        "--top-p", LLM_TOPP,
        "-ngl", LLM_NGL,
        "-p", f"{prompt_sys}\nJSON:\n{user}\n",
        "--grammar", gpath.as_posix(),
    ]
    try:
        p = subprocess.run(cmd, cwd=str(LLAMA_BIN), capture_output=True, text=True, timeout=LLM_TIMEOUT_S)
    except subprocess.TimeoutExpired:
        return {"ops":[]}
    except FileNotFoundError:
        # Binary not found at runtime; skip edits
        return {"ops":[]}
    out = (p.stdout or "").strip()
    m = re.search(r"\{.*\}\s*\Z", out, re.S)
    if not m:
        return {"ops":[]}
    try:
        js = json.loads(m.group(0))
        return js if isinstance(js, dict) and isinstance(js.get("ops", None), list) else {"ops":[]}
    except Exception:
        return {"ops":[]}

def apply_edit_script(df: pd.DataFrame, edits: dict) -> pd.DataFrame:
    if not isinstance(edits, dict) or "ops" not in edits:
        return df

    df = df.copy()
    cols = df.columns.tolist()
    value_cols = [c for c in cols if c != "Category"]

    def safe_row(i): 
        return 0 <= i < len(df)

    rows_to_add = []  # buffer for add_and_calculate_row

    for op in edits["ops"]:
        if not isinstance(op, dict) or "op" not in op:
            continue
        k = op.get("op")

        if k == "rename":
            i = int(op.get("row", -1)); to = _canon_label(op.get("to",""))
            if safe_row(i) and to and (to in WHITELIST):
                df.iloc[i, 0] = to

        elif k == "swap_columns":
            a = str(op.get("col_a","")); b = str(op.get("col_b",""))
            if a in cols and b in cols and a != "Category" and b != "Category":
                ia, ib = cols.index(a), cols.index(b)
                cols[ia], cols[ib] = cols[ib], cols[ia]
                df = df[cols]
                value_cols = [c for c in cols if c != "Category"]

        elif k == "fix_number":
            i = int(op.get("row",-1)); col = str(op.get("col",""))
            if safe_row(i) and col in df.columns and col != "Category":
                new = _to_number(str(op.get("to","")).strip())
                if isinstance(new,(int,float)) or (isinstance(new,str) and str(new).endswith("%")):
                    df.at[i, col] = new

        elif k == "fill_missing":
            i = int(op.get("row",-1)); col = str(op.get("col",""))
            if safe_row(i) and col in df.columns and col != "Category":
                val = _to_number(op.get("value", ""))
                if isinstance(val,(int,float)) or (isinstance(val,str) and str(val).endswith("%")):
                    df.at[i, col] = val

        elif k == "calculate_total":
            i = int(op.get("row",-1)); col = str(op.get("col",""))
            comps = op.get("components", [])
            if safe_row(i) and col in df.columns and col != "Category" and isinstance(comps, list):
                total = 0.0; have = False
                for comp in comps:
                    key_norm = _norm_key(_canon_label(comp))
                    m = df["Category"].map(_norm_key).eq(key_norm)
                    if m.any():
                        val = _to_number(df.loc[m, col].iloc[0])
                        if isinstance(val,(int,float)):
                            total += val; have = True
                if have:
                    df.at[i, col] = total

        elif k == "drop_row":
            i = int(op.get("row",-1))
            if safe_row(i):
                df = df.drop(df.index[i]).reset_index(drop=True)

        elif k == "add_and_calculate_row":
            cat_raw = str(op.get("category", "")).strip()
            idx = int(op.get("index", len(df)))
            col = str(op.get("col", "")).strip()
            comps = op.get("components", [])

            # Validate that the necessary keys exist and have valid values
            if not (cat_raw and col and comps and (col in df.columns) and (col != "Category")):
                continue

            cat_canon = _canon_label(cat_raw)

            total = 0.0
            touched = False

            # Calculate the sum of components
            for comp in comps:
                key_norm = _norm_key(_canon_label(comp))
                m = df["Category"].map(_norm_key).eq(key_norm)
                
                if m.any():
                    val = _to_number(df.loc[m, col].iloc[0])
                    if isinstance(val, (int, float)) and not pd.isna(val):
                        total += val
                        touched = True
            
            # If any components were found and summed, add the new row to the list
            if touched:
                row_dict = {"Category": cat_canon}
                for c in value_cols:
                    row_dict[c] = None
                row_dict[col] = total
                rows_to_add.append((max(0, min(idx, len(df))), row_dict))
        elif k == "derive_missing_value":
            cat = _canon_label(str(op.get("category", ""))).lower()
            col = str(op.get("col", ""))
            if "Category" not in df.columns or col not in df.columns or not cat:
                continue

            tmp = df.copy()
            tmp["__canon"] = tmp["Category"].astype(str).map(lambda s: _canon_label(s).lower())
            by = {tmp["__canon"].iloc[i]: i for i in range(len(tmp))}
            def _g(name):
                i = by.get(name)
                if i is None:
                    return None
                return _to_number(tmp.at[i, col])
            def _set(name, value):
                i = by.get(name)
                if i is None:
                    new = {"Category": name, **{c: None for c in df.columns if c != "Category"}}
                    df.loc[len(df)] = new
                    by[name] = len(df) - 1
                    i = by[name]
                df.at[i, col] = value

            if cat == "total non-operating gains (losses)":
                interest = _g("interest income, net")
                interest_e = _g("interest expense")
                loss_sale = _g("loss on sale of assets")
                gifts = _g("donations (gift)")
                other_mx = _g("other income/(expense)")
                other_inc = _g("other income")
                other_exp = _g("other expense")

                if isinstance(loss_sale, (int, float)) and loss_sale > 0:
                    loss_sale = -abs(loss_sale)

                total = 0.0; touched = False
                if isinstance(other_mx,(int,float)):
                    total += other_mx; touched = True
                else:
                    if isinstance(other_inc,(int,float)): total += other_inc; touched = True
                    if isinstance(other_exp,(int,float)): total -= other_exp; touched = True
                for v, sgn in [(interest,1.0),(interest_e,-1.0),(loss_sale,1.0),(gifts,1.0)]:
                    if isinstance(v,(int,float)): total += sgn*v; touched = True
                if touched: _set(cat, total)

            elif cat == "net income (loss)":
                opinc = _g("operating income")
                nonop = _g("total non-operating gains (losses)")
                tax = _g("provision for income taxes")
                if any(x is not None for x in (opinc, nonop, tax)):
                    _set(cat, (opinc or 0.0) + (nonop or 0.0) - (tax or 0.0))

            elif cat == "gross profit":
                ts = _g("total sales")
                cogs = _g("cost of goods sold") or _g("cost of revenue")
                if (ts is not None) and (cogs is not None):
                    _set(cat, ts - cogs)

            elif cat == "operating income":
                gp = _g("gross profit")
                opex = _g("operating expenses") or _g("total expenses")
                ts = _g("total sales")
                if (gp is not None) and (opex is not None):
                    _set(cat, gp - opex)
                elif (ts is not None) and (opex is not None):
                    _set(cat, ts - opex)

    # Correct and complete the final concatenation
    if rows_to_add:
        # Sort rows to be added by their insertion index
        rows_to_add.sort(key=lambda x: x[0])
        
        final_df_rows = []
        current_idx = 0
        for idx, row_dict in rows_to_add:
            # Append rows from the original DataFrame up to the insertion point
            if idx > current_idx:
                final_df_rows.append(df.iloc[current_idx:idx])
            # Append the new row
            final_df_rows.append(pd.DataFrame([row_dict], columns=df.columns))
            current_idx = idx
        
        # Append any remaining rows from the original DataFrame
        if current_idx < len(df):
            final_df_rows.append(df.iloc[current_idx:])

        df = pd.concat(final_df_rows, ignore_index=True)

    return df
# ========= CLI Workflow: Parse → LLM → Apply Ops → Export =========

def _build_llm_payload(df: pd.DataFrame) -> dict:
    """
    Build the JSON the LLM needs: the table, columns, whitelist/synonyms,
    and a 'suspects' list containing cells that should be derived.
    """
    if df is None or df.empty or "Category" not in df.columns:
        return {"table": [], "columns": [], "suspects": [], "whitelist": [], "synonyms": {}}

    table_for_llm = df.drop(columns=[c for c in df.columns if str(c).startswith("__")], errors="ignore").copy()
    val_cols = [c for c in table_for_llm.columns if c != "Category"]
    suspects = []

    # rows the LLM can/should derive, with their component lists
    TARGETS = {
        "total non-operating gains (losses)": ["interest income, net", "loss on sale of assets", "donations (gift)"],
        "net income (loss)": ["operating income", "total non-operating gains (losses)", "provision for income taxes"],
        # (optional) add these if you want the LLM to also fill them:
        # "total sales": ["client services revenue", "client service revenue", "book sales", "professional consultation"],
        # "total expenses": ["wages", "wages and benefits", "marketing and advertising", "rent", "utilities",
        #                    "memberships and publications", "insurance", "consultants", "office supplies"],
    }

    for i, row in table_for_llm.iterrows():
        canon = _canon_label(row["Category"]).lower()
        if canon in TARGETS:
            for c in val_cols:
                if pd.isna(row[c]) or row[c] == "":
                    suspects.append({
                        "row": int(i),
                        "col": str(c),
                        "reason": "missing_total",
                        "components": TARGETS[canon],
                    })

    payload = {
        "table": table_for_llm.replace({np.nan: None}).to_dict(orient="records"),
        "columns": table_for_llm.columns.tolist(),
        "suspects": suspects,
        "whitelist": sorted(list(WHITELIST)),
        "synonyms": SYNONYMS,
    }
    return payload


# def _build_auto_ops_for_missing_totals(df: pd.DataFrame) -> dict:
#     """
#     Deterministic ops for two critical lines when components exist:
#       - total non-operating gains (losses)
#       - net income (loss)
#     """
#     if df is None or df.empty or "Category" not in df.columns:
#         return {"ops": []}

#     ops = []
#     cat = df["Category"].astype(str).map(lambda s: _canon_label(s).lower())
#     have    = lambda k: cat.eq(k).any()
#     missing = lambda k: ~cat.eq(k).any()
#     val_cols = [c for c in df.columns if c != "Category" and not str(c).startswith("__")]

#     if (have("interest income, net") or have("loss on sale of assets") or have("donations (gift)")) \
#             and missing("total non-operating gains (losses)"):
#         comps = ["interest income, net", "loss on sale of assets", "donations (gift)"]
#         for col in val_cols:
#             ops.append({
#                 "op": "add_and_calculate_row",
#                 "category": "total non-operating gains (losses)",
#                 "index": len(df),
#                 "col": col,
#                 "components": comps
#             })

#     if (have("operating income") or have("total non-operating gains (losses)") or have("provision for income taxes")) \
#             and missing("net income (loss)"):
#         comps = ["operating income", "total non-operating gains (losses)", "provision for income taxes"]
#         for col in val_cols:
#             ops.append({
#                 "op": "add_and_calculate_row",
#                 "category": "net income (loss)",
#                 "index": len(df),
#                 "col": col,
#                 "components": comps
#             })

#     return {"ops": ops}


# def merge_llm_and_auto_ops(llm_edits: dict, df: pd.DataFrame) -> dict:
#     """Merge LLM ops with auto-ops; drop duplicates by JSON fingerprint."""
#     all_ops = []
#     seen = set()
#     for src in [llm_edits or {}, _build_auto_ops_for_missing_totals(df)]:
#         for op in src.get("ops", []):
#             key = json.dumps(op, sort_keys=True)
#             if key not in seen:
#                 seen.add(key)
#                 all_ops.append(op)
#     return {"ops": all_ops}




def _parse_single_page_to_df(path: str) -> pd.DataFrame:
    """
    Step 1: Parse OCR → DataFrame from a PDF/image (uses your existing functions).
    Prefers table detection, falls back to line parser, renames periods to years,
    unifies/cleans columns, collapses duplicates, and coarse ordering.
    """
    p = Path(path)
    pages = pdf_or_images_to_pages([p], dpi=220)
    if not pages:
        raise RuntimeError(f"No pages found in {path!r}")
    img = prepare_image(pages[0], MAX_LONG_SIDE)
    img = remove_underlines(img)

    years_hint = _detect_years_in_header(img)[:2]

    table_data, _ = detect_and_extract_table(img)
    df = parse_finance_table(table_data) if table_data else None
    if df is None or (hasattr(df, "empty") and df.empty):
        lines = ocr_lines(img)
        df, _ = parse_finance_lines(lines)

    if df is None or df.empty:
        raise RuntimeError("Parser returned an empty DataFrame")

    df = _rename_periods_to_years(df, years_hint)
    df = _drop_year_rows(df)
    if "_unify_columns" in globals():
        df = _unify_columns(df)
    df = _coerce_numeric_inplace(df)
    df = _collapse_duplicates(df)
    df = recompute_totals(df)
    df = order_like_statement(df)
    return df

def run_once_cli(input_path: str, output_path: str) -> str:
    """
    Full sequence:
      1) parse OCR → df
      2) call LLM → edit script (bounded)
      3) apply script
      4) finalize & export
    Returns path to saved workbook.
    """
    # Step 1
    df = _parse_single_page_to_df(input_path)

    # Step 2: bounded LLM fixer
    # Step 2: bounded LLM fixer
    with tempfile.TemporaryDirectory() as td:
        payload = _build_llm_payload(df)
        edits = run_llm_fixer(payload, Path(td))

    # Step 3: merge ops and apply
    merged_ops = _merge_llm_and_auto_ops(df, edits)
    fixed = apply_edit_script(df.copy(), merged_ops)

    # Optional: post-rename/post-creation second pass (often helps after LLM renames)
    post_auto = _build_auto_ops_for_missing_totals(fixed)
    if post_auto.get("ops"):
        fixed = apply_edit_script(fixed, post_auto)

    # Step 4: finalize & export (NO fill_nonop_and_net_income)
    fixed = _drop_year_rows(fixed)
    fixed = _coerce_numeric_inplace(fixed)
    fixed = _collapse_duplicates(fixed)
    fixed = recompute_totals(fixed)
    fixed = order_like_statement(fixed)
    fixed = fixed.drop(columns=["__section_rank", "__y", "__conf", "__norm", "__canon", "__k"], errors="ignore")
    for c in [x for x in fixed.columns if x != "Category"]:
        fixed[c] = pd.to_numeric(fixed[c], errors="coerce")

    return _safe_write_excel([fixed], output_path)


# -------------------- Excel helpers --------------------
def autosize_sheet(writer: pd.ExcelWriter, df: pd.DataFrame, sheet_name: str) -> None:
    ws = writer.sheets[sheet_name]
    widths = []
    for col in df.columns:
        series = df[col].astype(str)
        max_len = max([len(str(col))] + series.map(len).tolist())
        widths.append(min(60, max(10, max_len + 2)))

    if hasattr(ws, "set_column"):  # xlsxwriter
        for i, w in enumerate(widths):
            ws.set_column(i, i, w)
        return

    try:  # openpyxl
        from openpyxl.utils import get_column_letter
        for i, w in enumerate(widths, start=1):
            ws.column_dimensions[get_column_letter(i)].width = w
    except Exception:
        pass

def to_excel_multiple(dfs: List[pd.DataFrame], out_path: str):
    try:
        import xlsxwriter
        engine = "xlsxwriter"
    except Exception:
        engine = "openpyxl"

    with pd.ExcelWriter(out_path, engine=engine) as writer:
        for i, df in enumerate(dfs, 1):
            name = "Extracted" if i == 1 else f"Table {i}"

            df.to_excel(writer, index=False, sheet_name=name)
            autosize_sheet(writer, df, name)

            if engine == "xlsxwriter":
                ws   = writer.sheets[name]
                book = writer.book

                fmt_int    = book.add_format({'num_format': '#,##0'})
                fmt_float  = book.add_format({'num_format': '#,##0.00'})
                fmt_header = book.add_format({'bold': True})
                fmt_section= book.add_format({'bold': True, 'italic': True})

                ws.freeze_panes(1, 1)
                ws.set_row(0, None, fmt_header)

                for col_idx, col in enumerate(df.columns):
                    if col == "Category":
                        continue
                    series = df[col]
                    if pd.api.types.is_float_dtype(series):
                        ws.set_column(col_idx, col_idx, None, fmt_float)
                    elif pd.api.types.is_integer_dtype(series):
                        ws.set_column(col_idx, col_idx, None, fmt_int)
                    else:
                        coerced = pd.to_numeric(series, errors="coerce")
                        if coerced.notna().any():
                            if (coerced % 1 != 0).any():
                                ws.set_column(col_idx, col_idx, None, fmt_float)
                            else:
                                ws.set_column(col_idx, col_idx, None, fmt_int)

                for row_idx, cat in enumerate(df["Category"].astype(str), start=1):
                    if _canon_label(cat) in HEADERS_KEEP:
                        ws.set_row(row_idx, None, fmt_section)
def _build_llm_payload(df: pd.DataFrame) -> dict:
    """
    Build the JSON the LLM needs: the table, columns, whitelist/synonyms,
    and a 'suspects' list containing cells that should be derived.
    """
    if df is None or df.empty or "Category" not in df.columns:
        return {"table": [], "columns": [], "suspects": [], "whitelist": [], "synonyms": {}}

    table_for_llm = df.drop(columns=[c for c in df.columns if str(c).startswith("__")], errors="ignore").copy()
    val_cols = [c for c in table_for_llm.columns if c != "Category"]
    suspects = []

    # rows the LLM can/should derive, with their component lists
    TARGETS = {
        "total non-operating gains (losses)": ["interest income, net", "loss on sale of assets", "donations (gift)"],
        "net income (loss)": ["operating income", "total non-operating gains (losses)", "provision for income taxes"],
        # (optional) add these if you want the LLM to also fill them:
        # "total sales": ["client services revenue", "client service revenue", "book sales", "professional consultation"],
        # "total expenses": ["wages", "wages and benefits", "marketing and advertising", "rent", "utilities",
        #                    "memberships and publications", "insurance", "consultants", "office supplies"],
    }

    for i, row in table_for_llm.iterrows():
        canon = _canon_label(row["Category"]).lower()
        if canon in TARGETS:
            for c in val_cols:
                if pd.isna(row[c]) or row[c] == "":
                    suspects.append({
                        "row": int(i),
                        "col": str(c),
                        "reason": "missing_total",
                        "components": TARGETS[canon],
                    })

    payload = {
        "table": table_for_llm.replace({np.nan: None}).to_dict(orient="records"),
        "columns": table_for_llm.columns.tolist(),
        "suspects": suspects,
        "whitelist": sorted(list(WHITELIST)),
        "synonyms": SYNONYMS,
    }
    return payload


# -------------------- GUI --------------------
root = Tk()
root.title("PDF → Excel (OCR-first + LLM-fixer, offline)")

status = StringVar(value="Idle.")
progress_txt = StringVar(value="0%")

Label(root, text="1) Pick files   2) Run   3) Save Excel").pack(padx=10, pady=(10,6))
btn_pick = Button(root, text="Pick files", width=18)
btn_pick.pack(pady=4)
btn_run = Button(root, text="Run", width=18)
btn_run.pack(pady=8)

Label(root, textvariable=status).pack(pady=(2,0))
bar = ttk.Progressbar(root, orient="horizontal", length=420, mode="determinate", maximum=100)
bar.pack(pady=(2,0))
Label(root, textvariable=progress_txt).pack(pady=(0,10))

selected_files: List[Path] = []

def choose_files():
    global selected_files
    paths = filedialog.askopenfilenames(title="Choose PDF or image files",
                                        filetypes=[("PDF/Images","*.pdf;*.png;*.jpg;*.jpeg;*.webp")])
    if paths:
        selected_files = [Path(p) for p in paths]
        status.set(f"{len(selected_files)} file(s) selected")

btn_pick.config(command=choose_files)

def set_progress(done:int, total:int, msg:str):
    pct = int(round(100*done/max(1,total)))
    bar["value"] = pct
    progress_txt.set(f"{pct}%")
    status.set(f"{msg}  [{done}/{total}]")
    root.update_idletasks()

# -------------------- Main pipeline --------------------
def _unify_columns(merged: pd.DataFrame) -> pd.DataFrame:
    """
    Keep Category + up to two value columns (prefer YEAR/TTM/YTD).
    - Drops helper columns
    - Deduplicates columns (keeps first occurrence)
    - Never reindexes onto duplicate labels
    """
    if merged is None or merged.empty:
        return merged

    df = merged.copy()

    # drop helper cols
    df = df.loc[:, ~df.columns.astype(str).str.startswith("__")]

    # dedupe existing duplicate columns (keep first)
    df = df.loc[:, ~df.columns.duplicated()]

    # ensure Category first if present
    cols = [c for c in df.columns if c != "Category"]
    if "Category" in df.columns:
        df = df[["Category", *cols]]

    # choose up to two value columns – prefer year headers, then TTM/YTD, then first others
    val_cols = [c for c in df.columns if c != "Category"]

    pref_years = [c for c in val_cols if HDR_YEAR_RE.fullmatch(str(c))]
    pref_tags  = [c for c in val_cols if str(c).upper() in {"TTM", "YTD"}]

    keep_vals: List[str] = []
    for pool in (pref_years, pref_tags, val_cols):
        for c in pool:
            if c not in keep_vals:
                keep_vals.append(c)
            if len(keep_vals) == 2:
                break
        if len(keep_vals) == 2:
            break

    # Build final keep list; include only existing cols and keep unique order
    seen, keep = set(), []
    for c in (["Category"] + keep_vals):
        if c in df.columns and c not in seen:
            keep.append(c); seen.add(c)

    # If nothing to keep (shouldn't happen), just return Category
    if not keep:
        keep = ["Category"] if "Category" in df.columns else list(df.columns[:1])

    return df.loc[:, keep]


def _collapse_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Collapse by normalized category; pick first non-null numeric (visual top)."""
    if df.empty:
        return df.copy()

    df = df.copy()
    df["__norm"]  = df["Category"].map(_norm_cat_for_match)
    df["__canon"] = df["Category"].map(_canon_label)
    val_cols = [c for c in df.columns if c not in ("Category","__norm","__canon")]

    def chooser(g: pd.DataFrame) -> pd.Series:
        name = g["__canon"].iloc[0] if g["__canon"].iloc[0] in WHITELIST else g["Category"].iloc[0]
        row = {"Category": name}
        for c in val_cols:
            vals = [ _to_number(v) for v in g[c].tolist() ]
            nums = [v for v in vals if isinstance(v,(int,float))]
            row[c] = nums[0] if nums else None
        return pd.Series(row)

    out = (df.groupby("__norm", as_index=False, sort=False)
             .apply(chooser)
             .reset_index(drop=True))

    out = out.reindex(columns=["Category", *val_cols])
    return out

def ocr_lines(img: Image.Image) -> List[Dict]:
    """Fallback: line-level OCR with positional info."""
    arr = cv2.cvtColor(np.array(img), cv2.COLOR_RGB2BGR)
    df = pytesseract.image_to_data(arr, output_type=pytesseract.Output.DATAFRAME)
    if df is None or df.empty:
        return []
    df = df.dropna(subset=["text"])
    lines = []
    for (blk, par, lin), g in df.groupby(["block_num","par_num","line_num"]):
        g = g.sort_values(["left"])
        t = " ".join(str(x) for x in g["text"].tolist()).strip()
        if not t:
            continue
        conf = float(max(0.0, g["conf"].astype(float).replace(-1, 0).mean()))
        y = int(g["top"].min())
        x = int(g["left"].min())
        lines.append({"text": t, "conf": conf, "y": y, "x": x})
    lines.sort(key=lambda r: (r["y"], r["x"]))
    return lines



def parse_finance_lines(lines: List[Dict]) -> Tuple[pd.DataFrame, List[Dict]]:
    """
    Parse OCR lines into a tidy table.
    - Detect two-year header early and remove that header row from the body.
    - Keep section headers (Sales, Expenses, Non-operating ...) even w/o numbers.
    - Extract the last two numeric tokens per line as the two periods.
    """
    if not lines:
        return pd.DataFrame(columns=["Category","Period1","Period2","__y","__conf"]), []

    # ---- 1) Find the header row with years (first 12 lines) ----
    val_cols = ["Period1", "Period2"]
    header_line_index = -1
    for i, r in enumerate(lines[:min(12, len(lines))]):
        toks = r["text"].split()
        years = [t for t in toks if HDR_YEAR_RE.fullmatch(t)]
        if len(years) >= 2:
            val_cols = years[:2]
            header_line_index = i
            break

    # Remove the header row from subsequent parsing so years don't become a data row
    work_lines = lines[header_line_index+1:] if header_line_index != -1 else list(lines)

    # ---- 2) Parse body ----
    rows, suspects = [], []
    for r in work_lines:
        text = r["text"]
        if DROP_PAT.search(text):
            continue

        # Find numbers but ignore pure year tokens
        raw_all = re.findall(r"\(?-?\$?[\d,]+(?:\.\d+)?\)?(?:DR|%)?", text, re.I)
        raw_nums = [tok for tok in raw_all if not HDR_YEAR_RE.fullmatch(tok)]

        # Split category before the first relevant numeric token (if any)
        first_num_str = raw_nums[-2] if len(raw_nums) >= 2 else (raw_nums[-1] if raw_nums else "")
        split_at = text.rfind(first_num_str) if first_num_str else -1
        cat = text[:split_at].strip() if split_at != -1 else text.strip()
        cat = re.sub(r'[.\·\:\-\–\—\s]+$', '', cat)
        canon = _canon_label(cat)

        # Section headers to preserve
        is_header = canon in {"sales", "expenses", "non-operating gains (losses)"}

        # Pull numbers (keep % strings, coerce others)
        nums = [_to_number(tok) for tok in raw_nums]
        nums = [n for n in nums if isinstance(n,(int,float)) or (isinstance(n,str) and str(n).endswith("%"))]

        if is_header and not nums:
            rows.append([canon, None, None, r.get("y", 0), float(r.get("conf", 0.0))])
            continue
        if not nums:
            continue

        p1 = nums[-2] if len(nums) >= 2 else None
        p2 = nums[-1] if len(nums) >= 1 else None
        rows.append([canon, p1, p2, r.get("y", 0), float(r.get("conf", 0.0))])

        if r.get("conf", 100) < 55:
            suspects.append({"row": len(rows)-1, "cols": val_cols, "reason": "low_conf", "conf": r["conf"]})

    df = pd.DataFrame(rows, columns=["Category", *val_cols, "__y", "__conf"])
    return df, suspects



def _drop_year_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Removes any rows that look like just a year."""
    if df is None or df.empty or "Category" not in df.columns:
        return df
    
    is_year_row = df["Category"].astype(str).str.match(r"^\d{4}$")
    
    return df[~is_year_row]

def _coerce_numeric_inplace(df: pd.DataFrame) -> pd.DataFrame:
    """Attempts to convert all columns (except 'Category') to numeric."""
    if df is None or df.empty:
        return df
    
    for c in df.columns:
        if c != "Category":
            df[c] = pd.to_numeric(df[c], errors="coerce")
            
    return df



def _ensure_parent_dir(path_str: str) -> None:
    p = Path(path_str).expanduser()
    p.parent.mkdir(parents=True, exist_ok=True)

def _is_file_locked(path_str: str) -> bool:
    """
    Best-effort Windows check. Returns True if an existing file appears locked
    (e.g., open in Excel). Non-existent files return False.
    """
    p = Path(path_str)
    if not p.exists():
        return False
    try:
        with open(p, "r+b"):
            pass
        return False
    except PermissionError:
        return True
    except OSError:
        return True

def _safe_write_excel(dfs: List[pd.DataFrame], out_path: str, engine: str = None) -> str:
    """
    Writes to a temp .xlsx and then replaces the destination.
    If replacement is blocked (file is open), saves to a timestamped alternate path and returns it.
    """
    _ensure_parent_dir(out_path)
    out_p = Path(out_path)
    stem  = out_p.stem
    tmp_p = out_p.with_name(f"{stem}.tmp.{os.getpid()}.xlsx")

    if engine is None:
        try:
            import xlsxwriter  # noqa
            engine = "xlsxwriter"
        except Exception:
            engine = "openpyxl"

    with pd.ExcelWriter(tmp_p, engine=engine) as writer:
        for i, df in enumerate(dfs, 1):
            name = "Extracted" if i == 1 else f"Table {i}"
            df.to_excel(writer, index=False, sheet_name=name)
            autosize_sheet(writer, df, name)

            if engine == "xlsxwriter":
                ws   = writer.sheets[name]
                book = writer.book
                fmt_int   = book.add_format({'num_format': '#,##0'})
                fmt_float = book.add_format({'num_format': '#,##0.00'})
                fmt_hdr   = book.add_format({'bold': True})

                ws.set_row(0, None, fmt_hdr)
                ws.freeze_panes(1, 1)

                for col_idx, col in enumerate(df.columns):
                    if col == "Category":
                        continue
                    col_series = df[col]
                    coerced = pd.to_numeric(col_series, errors="coerce")
                    if coerced.notna().any():
                        if (coerced % 1 != 0).any():
                            ws.set_column(col_idx, col_idx, None, fmt_float)
                        else:
                            ws.set_column(col_idx, col_idx, None, fmt_int)

    try:
        os.replace(tmp_p, out_p)
        return str(out_p)
    except PermissionError:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        alt_p = out_p.with_name(f"{stem}_{ts}.xlsx")
        shutil.move(tmp_p, alt_p)
        return str(alt_p)

def run_pipeline():
    try:
        # --- pick files & output ---
        if not selected_files:
            messagebox.showwarning("PDF → Excel", "Pick at least one file first.")
            return

        out = filedialog.asksaveasfilename(
            title="Save Excel as…",
            defaultextension=".xlsx",
            filetypes=[("Excel","*.xlsx")],
            initialfile="extracted.xlsx"
        )
        if not out:
            return

        # --- 1) Input → images ---
        set_progress(0, 100, "Converting to images…")
        pages = pdf_or_images_to_pages(selected_files, dpi=220)
        if not pages:
            messagebox.showwarning("PDF → Excel", "No pages found.")
            return

        # --- 2) Page OCR (table-first, fallback to lines) ---
        years_hint: List[str] = []
        all_tables: List[pd.DataFrame] = []
        total_steps = len(pages) + 5
        done = 0

        for pi, pimg in enumerate(pages, 1):
            set_progress(done, total_steps, f"Processing page {pi}…")

            img = prepare_image(pimg, MAX_LONG_SIDE)
            img = remove_underlines(img)

            if not years_hint:
                years_hint = _detect_years_in_header(img)[:2]

            table_data, _ = detect_and_extract_table(img)
            df = parse_finance_table(table_data) if table_data else None

            if df is None or (hasattr(df, "empty") and df.empty):
                lines = ocr_lines(img)
                df, _ = parse_finance_lines(lines)

            if df is not None and not df.empty:
                df = _rename_periods_to_years(df, years_hint)
                all_tables.append(df)

            done += 1
            set_progress(done, total_steps, f"OCR done for page {pi}")

        if not all_tables:
            messagebox.showinfo("PDF → Excel", "No tables extracted.")
            set_progress(total_steps, total_steps, "Done.")
            return

        # --- 3) Merge → normalize → totals (pre-LLM) ---
        set_progress(done, total_steps, "Merging tables…")
        merged = pd.concat(all_tables, ignore_index=True)
        merged = _rename_periods_to_years(merged, years_hint)
        merged = _drop_year_rows(merged)

        merged = _unify_columns(merged)
        merged = _coerce_numeric_inplace(merged)

        merged = _collapse_duplicates(merged)
        merged = recompute_totals(merged)
        merged = order_like_statement(merged)

        suspects: List[Dict] = []
        val_cols = [c for c in merged.columns if c != "Category"]
        for i, row in merged.iterrows():
            lc = str(row["Category"]).lower()
            if "total" in lc or lc in {"total sales", "total expenses", "total non-operating gains (losses)"}:
                for c in val_cols:
                    if pd.isna(row[c]) or row[c] == "":
                        suspects.append({"row": int(i), "col": c, "reason": "missing_total"})

       # --- 4) LLM fixer (bounded ops) ---
        set_progress(done + 1, total_steps, "LLM fixer…")

        merged_for_llm = merged.drop(columns=[c for c in merged.columns if str(c).startswith("__")], errors="ignore").copy()
        with tempfile.TemporaryDirectory() as td:
            payload = _build_llm_payload(merged_for_llm)
            llm_edits = run_llm_fixer(payload, Path(td))

        # apply ONLY the edit script; do NOT re-compute totals elsewhere
        merged_fixed = apply_edit_script(merged_for_llm.copy(), llm_edits)


       # --- 5) Finalize: purge year rows, force numerics, keep headers, order ---
        set_progress(done + 2, total_steps, "Finalizing…")

        merged_fixed = _drop_year_rows(merged_fixed)
        merged_fixed = _coerce_numeric_inplace(merged_fixed)

        # normalize & collapse dups, but don't change the numbers
        merged_fixed["__norm"]  = merged_fixed["Category"].map(_norm_cat_for_match)
        merged_fixed["__canon"] = merged_fixed["Category"].map(_canon_label)
        merged_fixed = _collapse_duplicates(merged_fixed)

        # keep section headers even if otherwise empty; drop other all-NaN
        val_cols = [c for c in merged_fixed.columns if c != "Category"]
        def _is_section_header_row(row: pd.Series) -> bool:
            return _canon_label(row["Category"]) in {"sales", "expenses", "non-operating gains (losses)"}

        if val_cols:
            merged_fixed = (
                merged_fixed[
                    merged_fixed.apply(lambda r: _is_section_header_row(r) or pd.notna(r[val_cols]).any(), axis=1)
                ].reset_index(drop=True)
            )

        merged_fixed = order_like_statement(merged_fixed)

        # strip helpers; ensure numeric dtype for Excel
        merged_fixed = merged_fixed.drop(columns=["__section_rank", "__y", "__conf", "__norm", "__canon", "__k"], errors="ignore")
        for c in [x for x in merged_fixed.columns if x != "Category"]:
            merged_fixed[c] = pd.to_numeric(merged_fixed[c], errors="coerce")


        # --- 6) Save ---
        set_progress(done + 3, total_steps, "Saving Excel…")

        if _is_file_locked(out):
            messagebox.showwarning(
                "File is open",
                "The output workbook is currently open in Excel.\n"
                "Close it (or choose a new name) to overwrite.\n\n"
                "I'll save to a timestamped filename instead."
            )

        final_path = _safe_write_excel([merged_fixed], out)

        set_progress(total_steps, total_steps, "Done.")
        messagebox.showinfo("PDF → Excel", f"Saved:\n{final_path}")

    except Exception as e:
        try:
            set_progress(100, 100, "Error")
        except Exception:
            pass
        messagebox.showerror("Error", f"{e}\n\n{traceback.format_exc()}")

# ---------- Launch guards: GUI or CLI ----------
def run_gui():
    # use the GUI you already built above
    btn_run.config(command=run_pipeline)
    root.resizable(False, False)
    root.mainloop()

if __name__ == "__main__":
    import argparse, sys

    parser = argparse.ArgumentParser(description="PDF → Excel (OCR + LLM fixer)")
    parser.add_argument("--gui", action="store_true", help="Launch the Tkinter GUI")
    parser.add_argument("--cli", action="store_true", help="Run one-shot CLI workflow")
    parser.add_argument("input", nargs="?", help="Input PDF/image path (CLI)")
    parser.add_argument("output", nargs="?", help="Output .xlsx path (CLI)")
    args = parser.parse_args()

    # Default to GUI if no CLI args given
    if args.gui or (not args.cli and (args.input is None and args.output is None)):
        run_gui()
    else:
        if not args.input or not args.output:
            print("Usage (CLI): python app.py --cli <input.pdf|png|jpg> <output.xlsx>")
            sys.exit(2)
        # run_once_cli must be defined above (the 4-step sequence)
        saved = run_once_cli(args.input, args.output)
        print(f"✅ Saved: {saved}")
