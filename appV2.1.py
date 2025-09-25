# app.py — OCR-first + bounded LLM fixer → Excel
# - Table detection (OpenCV) + cell-based OCR (Tesseract)
# - Robust label normalization (synonyms, punctuation-insensitive)
# - Bounded LLM “fixer” with grammar (no invented rows/cols)
# - Preserves statement ordering & recomputes totals
# - Robust Excel autosize (xlsxwriter or openpyxl)
# - Tkinter GUI

# ---- Navigation (sections) ----
#   [S0] Imports & Constants
#   [S1] LLM Grammar & Paths
#   [S2] Numeric Parsing & Regex
#   [S3] Header Detection & Normalization
#   [S4] OCR & Layout Parsing
#   [S5] Table Reconstruction & Postprocess
#   [S6] Excel I/O (imported helpers)
#   [S7] GUI (Tkinter)
#   [S8] Pipeline (run_pipeline, run_gui, run_once_cli) + __main__
# --------------------------------
# Function Map (high-level):
#   S1 LLM: _llm_ready, _build_llm_payload, run_llm_fixer, apply_edit_script, _merge_llm_and_auto_ops
#   S2 Numeric: _num_like, _to_number_loose, _to_number_robust, _is_num_token, coerce_numeric
#   S3 Headers: _canon_col_name_v3, _merge_and_clean_headers, _detect_period_headers_xy, _get_headers_for_image
#   S4 OCR/Layout: pdf_or_images_to_pages, preprocess_for_ocr, process_image, parse_finance_table, parse_by_layout_v4
#   S5 Post: order_like_statement, _unify_columns, _collapse_duplicates, _process_parsed_data, finalize

# region [S0] Imports & Constants
import os, re, json, tempfile, subprocess, traceback
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
from pytesseract import Output
from tkinter import Tk, filedialog, Button, Label, StringVar, messagebox
from tkinter import ttk
# Optional scientific-image deps (not strictly required). If missing, we stub them.
try:
    from skimage.transform import rotate as _sk_rotate  # noqa: F401
    from skimage.color import rgb2gray as _sk_rgb2gray   # noqa: F401
    from skimage.feature import canny as _sk_canny       # noqa: F401
except Exception:
    _sk_rotate = None
    _sk_rgb2gray = None
    _sk_canny = None
try:
    from scipy.ndimage import sobel as _sk_sobel  # noqa: F401
except Exception:
    _sk_sobel = None

# region [S6] Excel I/O (imported helpers)
# Moved to module for clarity; behavior unchanged.
from pdf2excel.excel_io import _safe_write_excel, _is_file_locked
# endregion
# --- Optional fuzzy matching (safe fallback if not installed) ---
try:
    from thefuzz import fuzz  # pip install thefuzz[speedup]
except Exception:
    class _FuzzStub:
        @staticmethod
        def ratio(a, b): return 0
    fuzz = _FuzzStub()
# --- Qwen-VL availability check (do this AFTER LLAMA_EXE/MODEL_TXT are defined) ---




# endregion

# region [S1] LLM Grammar & Paths

def _build_llm_payload(df: pd.DataFrame) -> dict:
    """
    Build the JSON the LLM needs: the table, columns, whitelist/synonyms,
    and a 'suspects' list containing cells that should be derived.
    """
    # Ensure Category is a clean string column
    if "Category" in df.columns:
        df["Category"] = df["Category"].astype(object)        \
                                    .where(df["Category"].notna(), "") \
                                    .map(lambda x: "" if x is None else str(x))

    if df is None or df.empty or "Category" not in df.columns:
        return {"table": [], "columns": [], "suspects": [], "whitelist": [], "synonyms": {}}

    table_for_llm = df.drop(columns=[c for c in df.columns if str(c).startswith("__")], errors="ignore").copy()
    val_cols = [c for c in table_for_llm.columns if c != "Category"]
    suspects = []

    TARGETS = {
        # Existing totals
        "total non-operating gains (losses)": [
            "interest income, net", "loss on sale of assets", "donations (gift)",
            # optionally: "other income/(expense)" handled in derive
        ],
        "net income (loss)": [
            "operating income", "total non-operating gains (losses)", "provision for income taxes"
        ],

        # New “Products” totals
        "total medical products": [
            "customer #1", "customer #2", "customer #3", "customer #4", "other medical customers"
        ],
        "total industrial products": ["matthew", "mark", "luke", "john", "peter"],
        "total revenue": ["total medical products", "total industrial products"],
        "total aps, inc. revenue": ["total revenue"],
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

def _llm_ready() -> bool:
    """Return True only if both the llama exe and the model file exist."""
    try:
        return (
            MODEL_TXT is not None
            and LLAMA_EXE is not None
            and Path(MODEL_TXT).exists()
            and Path(LLAMA_EXE).exists()
        )
    except Exception:
        return False

def _merge_llm_and_auto_ops(df: pd.DataFrame, llm_edits: dict) -> dict:
    """Merge LLM ops with auto-ops; drop duplicates by JSON fingerprint."""
    all_ops = []
    seen = set()
    for src in (llm_edits or {}, _build_auto_ops_for_missing_totals(df)):
        for op in src.get("ops", []):
            key = json.dumps(op, sort_keys=True)
            if key not in seen:
                seen.add(key)
                all_ops.append(op)
    return {"ops": all_ops}

def apply_edit_script(df, edits):
    """
    Supports (at minimum) the 'add_and_calculate_row' op used by your test.
    - Category/component matching is case-insensitive via _canon_label/_norm_key
    - If the target row exists, it updates the value; otherwise inserts at index
    """
    import pandas as pd

    if not isinstance(df, pd.DataFrame) or not isinstance(edits, dict):
        return df
    ops = edits.get("ops", [])
    if not ops:
        return df

    out = df.copy()

    # Helper index: normalized canon -> row index (first match wins)
    def _canon_series(s):
        return s.astype(str).map(_canon_label).map(_norm_key)

    def _row_index_by_name(name):
        canon = _norm_key(_canon_label(name))
        m = _canon_series(out["Category"]).eq(canon)
        return int(m.idxmax()) if m.any() else None

    for op in ops:
        if not isinstance(op, dict):
            continue
        if op.get("op") != "add_and_calculate_row":
            # ignore other ops for this test-focused implementation
            continue

        cat_raw = str(op.get("category", "")).strip()
        col     = str(op.get("col", "")).strip()
        comps   = op.get("components", []) or []
        index   = op.get("index", len(out))

        if not cat_raw or not col or col == "Category" or col not in out.columns:
            continue

        # Sum the available component values (case-insensitive)
        total = 0.0
        touched = False
        for comp in comps:
            ri = _row_index_by_name(comp)
            if ri is None:
                continue
            v = to_number(out.at[ri, col])
            if isinstance(v, (int, float)):
                total += v
                touched = True

        if not touched:
            continue

        # Insert or update the target row
        tgt_i = _row_index_by_name(cat_raw)
        if tgt_i is None:
            # build a new row with NaNs, set the computed col
            newrow = {c: None for c in out.columns}
            newrow["Category"] = _canon_label(cat_raw)
            newrow[col] = total
            index = max(0, min(int(index), len(out)))
            upper = out.iloc[:index]
            lower = out.iloc[index:]
            out = pd.concat([upper, pd.DataFrame([newrow], columns=out.columns), lower], ignore_index=True)
        else:
            out.at[tgt_i, col] = total

    return out

def run_llm_fixer(payload: dict, tmpdir: Path) -> dict:
    # If the model isn’t configured/available, just skip
    if not _llm_ready():
        # Optional: print so you can see it in console
        print("LLM fixer not available (MODEL_TXT/LLAMA_EXE missing) – skipping.")
        return {"ops": []}

    gpath = tmpdir / "edit.gbnf"
    gpath.write_text(JSON_GRAMMAR.strip(), encoding="utf-8")

    prompt_sys = (
        "You are a STRICT financial table reasoner. Reconstruct a valid financial statement "
        "from the provided table. Use logic only from data present; NEVER invent rows/columns.\n"
        "Output ONLY JSON with 'ops' constrained by the grammar. Allowed ops: rename (to whitelist/synonyms), "
        "swap_columns, fix_number, fill_missing, calculate_total, add_and_calculate_row, derive_missing_value.\n"
        "Temperature must be 0."
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
        return {"ops": []}
    except FileNotFoundError:
        # exe vanished or path wrong — also skip
        print("LLM exe not found; skipping fixer.")
        return {"ops": []}

    out = (p.stdout or "").strip()
    m = re.search(r"\{.*\}\s*\Z", out, re.S)
    if not m:
        return {"ops": []}
    try:
        js = json.loads(m.group(0))
        return js if isinstance(js, dict) and isinstance(js.get("ops", None), list) else {"ops": []}
    except Exception:
        return {"ops": []}

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
# Central debug directory: HARD-WIRED for reliability
# All diagnostics are written here regardless of working directory.
HARD_DEBUG_DIR = Path(r"D:\Pdf2ExcelOffline\__debug__")
try:
    HARD_DEBUG_DIR.mkdir(parents=True, exist_ok=True)
except Exception:
    pass
# Ensure all debug routines respect this absolute path
DEBUG_DIR = HARD_DEBUG_DIR


# Tesseract (Windows)
pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"
MODEL_TXT = Path(r"D:\Pdf2ExcelOffline\models\qwen2.5-vl-7b-instruct-q4_k_m.gguf")
LLAMA_EXE = Path(r"D:\Pdf2ExcelOffline\llama-bin\llama-mtmd-cli.exe")
try:
    MODEL_TXT
except NameError:
    MODEL_TXT = None

try:
    LLAMA_EXE
except NameError:
    LLAMA_EXE = None

# -------------------- OCR / LLM KNOBS --------------------
OCR_CONF_THRESH   = 30   # was 40; too strict for scanned prints
MAX_LONG_SIDE     = 2400           # higher for small fonts in screenshots
LAYOUT_OCR_CONF = 5  # allow very low-conf words for labels
TSV_CFG = "--psm 6 --oem 1 -c preserve_interword_spaces=1"
LLM_TIMEOUT_S     = 120
LLM_CTX           = 2048
LLM_TEMP          = "0"
LLM_TOPK          = "1"
LLM_TOPP          = "0"
LLM_NGL           = "0"            # CPU for stability
DEFAULT_THREADS   = str(os.cpu_count() or 8)
DEBUG_MODE = True
DEBUG_SAVE_ROIS = True   # set True to save every OCR ROI crop

def preprocess_for_ocr(img: Image.Image) -> Image.Image:
    """Minimal, safe preprocessing: grayscale + light denoise.
    This avoids over-aggressive transforms that can obliterate left labels.
    """
    try:
        arr = np.array(img.convert('L'))
        # very light blur to reduce speckle without losing strokes
        try:
            arr = cv2.GaussianBlur(arr, (3,3), 0)
        except Exception:
            pass
        return Image.fromarray(arr)
    except Exception:
        return img.convert('L')

def _preprocess_for_ocr(img: Image.Image) -> Image.Image:
    """
    Prepares an image for OCR by converting to grayscale, de-skewing and thresholding.
    Uses OpenCV for robust angle estimation from a binary mask.
    """
    try:
        # PIL -> OpenCV gray
        img_cv = np.array(img.convert('RGB'))
        img_gray = cv2.cvtColor(img_cv, cv2.COLOR_BGR2GRAY)

        # Binary for skew estimation
        _, thresh = cv2.threshold(img_gray, 0, 255, cv2.THRESH_BINARY_INV | cv2.THRESH_OTSU)
        coords = np.column_stack(np.where(thresh > 0))
        if coords.size == 0:
            # fallback: simple threshold
            _, bw = cv2.threshold(img_gray, 0, 255, cv2.THRESH_BINARY | cv2.THRESH_OTSU)
            return Image.fromarray(bw).convert('L')

        angle = cv2.minAreaRect(coords)[-1]
        if angle < -45:
            angle = -(90 + angle)
        else:
            angle = -angle

        (h, w) = img_gray.shape
        center = (w // 2, h // 2)
        M = cv2.getRotationMatrix2D(center, angle, 1.0)
        img_rot = cv2.warpAffine(img_gray, M, (w, h), flags=cv2.INTER_CUBIC, borderMode=cv2.BORDER_REPLICATE)
        # Final clean threshold
        _, bw = cv2.threshold(img_rot, 0, 255, cv2.THRESH_BINARY | cv2.THRESH_OTSU)
        return Image.fromarray(bw).convert('L')
    except Exception:
        try:
            _, bw = cv2.threshold(np.array(img.convert('L')), 0, 255, cv2.THRESH_BINARY | cv2.THRESH_OTSU)
            return Image.fromarray(bw).convert('L')
        except Exception:
            return img.convert('L')

def simple_ocr_test(img: Image.Image):
    """
    Bypasses complex logic to perform raw OCR and print results.
    Saves the preprocessed image for inspection in DEBUG_DIR.
    """
    print("\n" + "="*30)
    print("  RUNNING SIMPLE OCR RAW TEXT TEST")
    print("="*30)

    # 1) OCR on grayscale image
    print("\n--- OCR Result on Grayscale Image ---")
    try:
        grayscale_text = pytesseract.image_to_string(img.convert('L'))
        print(grayscale_text)
    except Exception as e:
        print(f"ERROR running OCR on grayscale image: {e}")

    # 2) OCR on your preprocessed image
    print("\n--- OCR Result on Your Preprocessed Image ---")
    try:
        preprocessed_img = preprocess_for_ocr(img)
        preprocessed_text = pytesseract.image_to_string(preprocessed_img)
        print(preprocessed_text)

        # Save preprocessed image for visual inspection
        try:
            DEBUG_DIR.mkdir(parents=True, exist_ok=True)
            preprocessed_img.save(DEBUG_DIR / "p01_preprocessed_for_test.png")
            print(f"\n[INFO] Saved your preprocessed image for inspection at: {DEBUG_DIR / 'p01_preprocessed_for_test.png'}")
        except Exception:
            pass
    except Exception as e:
        print(f"ERROR running preprocess_for_ocr: {e}")

    # 3) OCR on LEFT STRIP with label-friendly config
    print("\n--- OCR Result on Left Strip (Label Config) ---")
    try:
        # Estimate first column x via header detection; fallback to 45% width
        left_x = None
        try:
            pairs = detect_period_columns_xy(img) or []
            if pairs:
                left_x = int(min(x for _lab, x in pairs))
        except Exception:
            left_x = None
        if left_x is None or left_x <= 0:
            left_x = int(max(20, img.width * 0.45))

        left_strip = img.crop((0, 0, max(10, left_x - 10), img.height))
        try:
            DEBUG_DIR.mkdir(parents=True, exist_ok=True)
            left_strip.convert('L').save(DEBUG_DIR / 'p01_left_strip_test.png')
        except Exception:
            pass
        txt_left = pytesseract.image_to_string(left_strip, config=LAB_TESS_CFG)
        print(txt_left)
    except Exception as e:
        print(f"ERROR left-strip OCR: {e}")

    print("\n" + "="*30)
    print("  TEST COMPLETE")
    print("="*30 + "\n")

# Column anchoring: when True, treat numeric columns as right-aligned.
# We then use token right-edges for snapping and draw an additional
# right-anchor overlay for visual confirmation.
ALIGN_NUMS_RIGHT = True

# -------------------- Statement ordering --------------------
# -------------------- Statement ordering --------------------
ORDER = [
    # --- "Products" sheet structure ---
    "medical products revenue",
    "customer #1", "customer #2", "customer #3", "customer #4", "other medical customers",
    "total medical products",

    "industrial products revenue",
    "matthew", "mark", "luke", "john", "peter",
    "total industrial products",

    "total revenue",
    "total aps, inc. revenue",

    # --- Original Income Statement structure ---
    "sales",
    "client service revenue", "book sales", "professional consultation",
    "total sales",

    "expenses",
    "wages", "marketing and advertising", "rent", "utilities",
    "memberships and publications", "insurance", "consultants", "office supplies",
    "total expenses",

    "operating income",

    "non-operating gains (losses)",
    "interest income, net", "loss on sale of assets", "donations (gift)",
    "total non-operating gains (losses)",

    "provision for income taxes",
    "net income (loss)",
]

ORDER_RANK = {k: i for i, k in enumerate(ORDER)}
CANONICAL  = list(dict.fromkeys(ORDER))  # unique, preserve order

SYNONYMS = {
    # Original
    "client services revenue": "client service revenue",
    "donations gift": "donations (gift)",
    "net income loss": "net income (loss)",
    "interest income net": "interest income, net",
    "interest income": "interest income, net",
    "income tax expense": "provision for income taxes",
    "loss on disposal of assets": "loss on sale of assets",
    "operating income (loss)": "operating income",
    "total operating expenses": "total expenses",

    # Products sheet
    "medical products": "medical products revenue",
    "industrial products": "industrial products revenue",
    "other medical": "other medical customers",
    "total aps inc revenue": "total aps, inc. revenue",
}
SYNONYMS.update({
    "inc revenue": "total aps, inc. revenue",
    "aps revenue": "total aps, inc. revenue",
    "aps, inc revenue": "total aps, inc. revenue",
    "aps inc revenue": "total aps, inc. revenue",
    "aps inc. revenue": "total aps, inc. revenue",
    "customer 1": "customer #1", "customer # 1": "customer #1",
    "customer 2": "customer #2", "customer # 2": "customer #2",
    "customer 3": "customer #3", "customer # 3": "customer #3",
    "customer 4": "customer #4", "customer # 4": "customer #4",
})
WHITELIST = set(CANONICAL) | set(SYNONYMS.values())
# --- Header inference fallbacks ---------------------------------------------
import re
from statistics import median

_NUMLIKE = re.compile(r"""
    ^\(?\s*            # optional opening paren
    (?:-|\+)?\s*       # optional sign
    (?:\d{1,3}(?:,\d{3})*|\d+)   # 1,234 or 1234
    (?:\.\d+)?         # .45
    \s*\)?$            # optional closing paren
""", re.X)

_MONTH = r"(?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*"
_MONTH_COL_RE = re.compile(rf"\b{_MONTH}(?:[-/\s]?\d{{2,4}})?\b", re.I)
_YTD_RE       = re.compile(r"\bYTD(?:\s+(?:Actual|Forecast))?\b", re.I)
_FORECAST_RE  = re.compile(r"\b(?:Forecast|Est(?:imate)?)\b", re.I)
_CUST_RE = re.compile(r"\bcust\w*\b", re.I)
_DIGIT_RE = re.compile(r"\b([1-4])\b")
NUM_TESS_CFG = (
    '--oem 1 --psm 6 '
    '-c tessedit_char_whitelist="0123456789,.-() " '
    '-c classify_bln_numeric_mode=1'
)
LAB_TESS_CFG = (
    '--oem 1 --psm 4 '
    '-c tessedit_char_whitelist="0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ#,.()- "'
)

# endregion

# region [S2] Numeric Parsing & Regex

def _best_num_from_roi(roi_gray: np.ndarray, target_local_x: float, *, strategy: str = "nearest") -> Optional[float]:
    """OCR ROI to data and return numeric token whose center-x is nearest target_local_x.
    Returns None if no numeric tokens found.
    """
    try:
        df = pytesseract.image_to_data(
            roi_gray,
            output_type=Output.DATAFRAME,
            config=("--psm 6 --oem 1 "
                    "-c preserve_interword_spaces=1 "
                    "-c tessedit_char_whitelist=\"0123456789,.-() \"")
        )
    except Exception:
        return None
    try:
        df = df.dropna(subset=["text"])  # type: ignore
    except Exception:
        return None
    # Collect numeric token candidates with geometry
    toks = []
    for _, r in df.iterrows():
        s = str(r.get("text", "")).strip()
        v = to_number(s)
        if not isinstance(v, (int, float)):
            continue
        try:
            left = float(r.get("left", 0)); width = float(r.get("width", 0))
        except Exception:
            continue
        cx = left + width / 2.0
        toks.append({"text": s, "val": v, "cx": cx, "left": left, "right": left + width})

    if not toks:
        return None

    # Optionally stitch a lone leading digit with the right-most chunk (fixes "5" + "20,219")
    toks_sorted = sorted(toks, key=lambda t: t["cx"])  # left->right
    try:
        rightmost = toks_sorted[-1]
        # Look left for a small one-digit token very close to the rightmost
        if len(toks_sorted) >= 2:
            left_near = toks_sorted[-2]
            gap = rightmost["left"] - left_near["right"]
            if gap <= 10 and len(re.sub(r"\D", "", left_near["text"])) == 1:
                combo = (left_near["text"] + rightmost["text"]).replace(" ", "")
                v_combo = to_number(combo)
                if isinstance(v_combo, (int, float)):
                    # Prefer the stitched value if it has more digits
                    def _nd(v):
                        try:
                            return len(str(int(abs(v))))
                        except Exception:
                            return 0
                    if _nd(v_combo) > _nd(rightmost["val"]):
                        rightmost = {**rightmost, "val": v_combo}
                        toks_sorted[-1] = rightmost
    except Exception:
        pass

    if strategy == "right":
        # Return the right-most numeric (after stitching)
        return toks_sorted[-1]["val"]

    # Otherwise choose nearest to target x
    best = min(toks_sorted, key=lambda t: abs(t["cx"] - float(target_local_x)))
    return best["val"]

def is_numeric_string(s: str) -> bool:
    """A single, robust function to check if a string is numeric."""
    if s is None:
        return False
    text = str(s).strip()
    if not text:
        return False
    return bool(NUM_TOKEN_SLOPPY.fullmatch(text.replace(" ", "")))

def to_number(s: str):
    """
    Converts '562,388' or '3.246.998' or '(1,234)' -> float.
    Treats multi-dot tokens with no commas as thousands dots.
    """
    if s is None: return None
    s = str(s).strip()
    neg = s.startswith("(") and s.endswith(")")
    s = s.strip("()")
    # normalize O/0
    s = s.replace("O", "0").replace("o", "0")
    # if it looks like 3.246.998 or 1.234.567,89
    if s.count(".") > 1 and "," not in s:
        s = s.replace(".", "")
    # now remove thousands commas
    s = s.replace(",", "")
    # remove stray characters
    s = re.sub(r"[^0-9.\-]", "", s)
    if s in {"", ".", "-"}:
        return None
    try:
        v = float(s)
        if neg: v = -v
        return v
    except Exception:
        return None

def coerce_numeric(df: pd.DataFrame) -> pd.DataFrame:
    """Coerce all non-Category columns to numeric (NaN on failure)."""
    if df is None or df.empty:
        return df
    for c in (col for col in df.columns if col != "Category"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

# robust numeric token check (for safety, though the whitelist already enforces this)
NUM_LAX = re.compile(r"^[()\-\s]*\d[\d\s,.\-()]*$")
CUSTOMER_RE = re.compile(r'customer\s*#?\s*(\d+)', re.I)
NUM_TOKEN_RE = re.compile(
    r"""
    ^\(?              # optional opening paren
    -?                # optional minus
    \$?               # optional leading $
    (?:
        \d{1,3}(?:[,.]\d{3})+  # 1,234,567 or 1.234.567
        |\d+                    # or just digits
    )
    (?:\.\d+)?         # optional decimal
    \)?$               # optional closing paren
    """,
    re.VERBOSE,
)


def _is_month_col(name: str) -> bool:
    s = str(name or '').lower()
    return bool(
        re.search(r"\b(jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)\b", s)
        or "forecast" in s
        or "ytd" in s
    )


def _score_products_df(df: Optional[pd.DataFrame]) -> float:
    """
    Heuristic quality score for a parsed products table.
    Lower is better. Penalizes tiny values in month columns and
    mismatches between totals and component sums.
    """
    import math
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return 1e9
    try:
        d = df.copy()
        if "Category" not in d.columns:
            return 1e9
        # Identify month-like columns (Jun-25, May-25, Forecast)
        month_cols = [c for c in d.columns if c != "Category" and _is_month_col(c)]
        # Penalty: small values (<100) in month columns (strong weight)
        small = 0
        for c in month_cols:
            vals = pd.to_numeric(d[c], errors="coerce")
            small += int((vals.notna()) & (vals.abs() < 100)).sum()

        # Component totals check (medical + industrial)
        canon = d["Category"].map(lambda s: _canon_label(s or ""))
        idx = {canon.iloc[i]: i for i in range(len(canon))}
        med_parts = ["customer #1","customer #2","customer #3","customer #4","other medical customers"]
        med_total = "total medical products"
        ind_parts = ["matthew","mark","luke","john","peter"]
        ind_total = "total industrial products"

        def _mismatch(parts, total_label):
            if total_label not in idx:
                return 2.0
            tot_row = d.loc[idx[total_label]]
            pen = 0.0
            for c in month_cols:
                try:
                    s = 0.0
                    for p in parts:
                        if p in idx:
                            v = pd.to_numeric(d.at[idx[p], c], errors="coerce")
                            if not (isinstance(v, (int, float)) or (hasattr(v, 'item') and isinstance(v.item(), (int,float)))):
                                v = float(v)
                            if not (pd.isna(v)):
                                s += float(v)
                    t = pd.to_numeric(tot_row[c], errors="coerce")
                    if pd.isna(t) or t == 0:
                        pen += 0.5
                    else:
                        rel = abs(float(t) - s) / (abs(float(t)) + 1e-6)
                        pen += min(2.0, rel)
                except Exception:
                    pen += 1.0
            return pen / max(1, len(month_cols))

        # Heavier penalty for any tiny month numbers — these indicate header/footer leakage
        penalty = small * 10.0 + _mismatch(med_parts, med_total) + _mismatch(ind_parts, ind_total)
        # Normalize by number of rows to avoid bias
        return float(penalty + 0.01 * len(d))
    except Exception:
        return 1e9


def _sanitize_small_month_values(df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    """Set tiny month/forecast values (<100) to NaN to suppress header/footnote leaks."""
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return df
    try:
        out = df.copy()
        month_cols = [c for c in out.columns if c != "Category" and _is_month_col(c)]
        for c in month_cols:
            s = pd.to_numeric(out[c], errors="coerce")
            mask = s.notna() & (s.abs() < 100)
            out.loc[mask, c] = pd.NA
        return out
    except Exception:
        return df


def _merge_prefer_filled(df_base: Optional[pd.DataFrame], df_other: Optional[pd.DataFrame], *, only_months: bool = True) -> Optional[pd.DataFrame]:
    """
    Fill NaNs in df_base with values from df_other by matching canonical Category.
    If only_months=True, restrict to month/forecast-like columns.
    """
    if df_base is None or not isinstance(df_base, pd.DataFrame) or df_base.empty:
        return df_other
    if df_other is None or not isinstance(df_other, pd.DataFrame) or df_other.empty:
        return df_base
    try:
        a = df_base.copy()
        b = df_other.copy()
        if 'Category' not in a.columns or 'Category' not in b.columns:
            return df_base
        a['_key'] = a['Category'].map(lambda s: _canon_label(s or ''))
        b['_key'] = b['Category'].map(lambda s: _canon_label(s or ''))
        b = b.drop_duplicates(subset=['_key'])
        # columns to consider
        cols = [c for c in a.columns if c != 'Category']
        if only_months:
            cols = [c for c in cols if _is_month_col(c)]
        # left join on key and fill
        m = a.merge(b[['_key', *[c for c in cols if c in b.columns]]], on='_key', how='left', suffixes=('', '_b'))
        for c in cols:
            cb = f"{c}_b"
            if cb in m.columns:
                m[c] = m[c].where(m[c].notna(), m[cb])
        a_out = m[[c for c in a.columns if c != '_key']]
        return a_out
    except Exception:
        return df_base


 
 



def _rewrite_layout_overlay(img: Image.Image, page_num: int) -> None:
    """
    Draw both column (vertical) and row (horizontal) anchors for diagnostics.
    Saves to DEBUG_DIR/pXX_layout.png
    """
    try:
        from PIL import ImageDraw
        DEBUG_DIR.mkdir(parents=True, exist_ok=True)
        dbg = img.convert("RGB").copy()
        drw = ImageDraw.Draw(dbg)
        H, W = dbg.height, dbg.width

        # 1) Column anchors (green)
        col_centers = []
        try:
            pairs = detect_period_columns_xy(img) or []
            col_centers = [float(x) for _, x in pairs]
            print("[DEBUG] Col centers:", [int(x) for x in col_centers])
        except Exception:
            pass
        for cx in col_centers:
            drw.line([(cx, 0), (cx, H)], fill=(0, 220, 0), width=2)

        # 2) Row anchors from left labels (orange)
        row_ys = []
        try:
            lines = ocr_lines(img)
            first_col_x = min(col_centers) if col_centers else W
            for ln in (lines or []):
                toks = ln.get('tokens') or []
                left_words = [t.get('t','') for t in toks if int(t.get('x',0)) < int(first_col_x - 15)]
                if not left_words:
                    continue
                cat = _canon_label(" ".join(w for w in left_words if w))
                if not cat or _is_header_label(cat):
                    continue
                row_ys.append(int(ln.get('y', 0)))
        except Exception:
            pass
        row_ys = sorted(set(int(y) for y in row_ys))
        print("[DEBUG] Row ys:", row_ys)
        for y in row_ys:
            drw.line([(0, y), (W, y)], fill=(255, 100, 0), width=1)

        dbg.save(DEBUG_DIR / f"p{page_num:02d}_layout.png")
    except Exception as e:
        print(f"[ERROR] Could not write debug overlay: {e}")







 


# Rows to keep even without numbers
HEADERS_KEEP = {
    "sales", "expenses", "non-operating gains (losses)",
    "medical products revenue", "industrial products revenue", "total revenue",
    # Always keep totals even if OCR numbers are missing
    "total medical products", "total industrial products", "total aps, inc. revenue",
}
HEADER_FIXES = (
    ("Yip", "YTD"),
    ("Ytp", "YTD"),
    ("Y1D", "YTD"),
    ("YID", "YTD"),
    ("Y1p", "YTD"),
)
MONTH_RE = re.compile(r"^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*-?\d{2}$", re.I)

# Helpers to detect accidental "year rows"
YEARS_PAIR_RE = re.compile(r"^\s*(19|20)\d{2}\s+(19|20)\d{2}\s*$")
# -------------------- Regex + helpers --------------------
try:
    RESAMPLE = Image.Resampling.LANCZOS
except AttributeError:
    RESAMPLE = Image.LANCZOS

# YEAR         = re.compile(r"^(19|20)\d{2}$")
# HDR_YEAR_RE  = re.compile(r"^(19|20)\d{2}$")
NUM = re.compile(r"^\(?-?\$?[\d,]+(?:\.\d+)?\)?(?:DR|%)?$")
DROP_PAT     = re.compile(r"(years?\s+ended|statement of|unaudited|page\s*\d+)", re.I)
# --- Sloppy numeric token that allows spaces inside () and supports DR/%, $ ---
# NUM_TOKEN_SLOPPY = re.compile(r"""
#     \(\s*-?\$?[\d,]+(?:\.\d+)?\s*\)   # ( 5,000 ) or (5,000)
#   | -?\$?[\d,]+(?:\.\d+)?(?:DR|%)?    # 5,000 | -417 | 12.5% | 350DR
# """, re.I | re.X)
NUM_TOKEN_SLOPPY = re.compile(r"""
^\(?\s*                 # optional opening paren
-?\$?\s*                 # optional leading minus or $
(?:
   \d{1,3}(?:[ ,\.\u00A0]\d{3})+   # 1,234,567 or 1 234 567 or 1.234.567
 | \d+                                 # or just digits
)
(?:[\.,]\d+)?             # optional decimal part , or .
\s*\)?$                    # optional closing paren
""", re.X)
# Period headers we might see across the top of wide sheets


# endregion

# region [S3] Header Detection & Normalization

def _canon_col_name_v3(s: str) -> str:
    if not s:
        return ""
    s = s.strip()
    s = s.replace("–", "-").replace("—", "-")
    s = re.sub(r"[,:;.\s]+$", "", s)  # strip trailing punctuation/spaces
    low = s.lower()

    # Common OCR glitches → canonical forms
    if re.search(r"\byt[dpl]\s*a(?:ct|ctual)?\b", low) or "yip actual" in low:
        return "YTD Actual"
    if re.search(r"\byt[dpl]\s*f(?:ore|orecast)?\b", low) or "yip forecast" in low:
        return "YTD Forecast"
    if re.search(r"\bjun[e]?\s*f(?:ore|orecast)?\b", low):
        return "June Forecast"

    m = re.search(r"(?i)\b(jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*\s*[-/ ]\s*(\d{2,4})\b", s)
    if m:
        mon = m.group(1)[:3].capitalize()
        yy  = m.group(2)
        if len(yy) == 4: yy = yy[-2:]
        return f"{mon}-{yy}"
    return s

def _compute_header_bottom_y(ocr_df: pd.DataFrame) -> float:
    """
    Estimate the bottom Y of the header band by finding lines containing
    period keywords (Jun/May/YTD/Forecast). Returns a Y coordinate; numbers
    above this are treated as header artifacts.
    """
    try:
        if ocr_df is None or ocr_df.empty:
            return -1.0
        tmp = ocr_df.dropna(subset=["text"]).copy()
        tmp["text"] = tmp["text"].astype(str)
        bottoms = []
        for (_, _), g in tmp.groupby(["block_num", "line_num"]):
            line_txt = " ".join(g["text"].tolist()).lower()
            if any(k in line_txt for k in ["jun", "may", "ytd", "forecast"]):
                try:
                    b = float((g["top"] + g["height"]).max())
                    bottoms.append(b)
                except Exception:
                    pass
        if bottoms:
            return max(bottoms)
        # fallback: use 15th percentile of token bottoms
        ys = (tmp["top"] + tmp["height"]).astype(float)
        return float(np.percentile(ys, 15)) if not ys.empty else -1.0
    except Exception:
        return -1.0

def _canon_label(s: str) -> str:
    raw = "" if s is None or (isinstance(s, float) and np.isnan(s)) else str(s)
    # Merge customerish normalization here to avoid extra helper
    low = re.sub(r"[^a-z0-9 #]+", " ", raw.lower())
    if _CUST_RE.search(low):
        m = _DIGIT_RE.search(low)
        if m:
            return f"customer #{m.group(1)}"
    k = _norm_key(raw)
    k = NORM_SYNONYMS.get(k, k)
    if k in CANON_BY_NORM:
        return CANON_BY_NORM[k]
    candidates = WHITELIST | set(SYNONYMS.values())
    best_name, best_score = None, -1
    for cand in candidates:
        score = fuzz.ratio(k, _norm_key(cand))
        if score > best_score:
            best_name, best_score = cand, score
    return best_name if best_score >= 80 else k

def _canonical_month_yy(mon: str, yy: str) -> str:
    mon = mon.lower()[:3]
    mon = mon.capitalize()  # 'jun' -> 'Jun'
    return f"{mon}-{yy}"

def _clean_hdr_text(s: str) -> str:
    s = (s or "").strip()
    s = s.replace("Ytp", "YTD")  # common OCR glitch
    s = re.sub(r"\s+", " ", s)
    return s

def _clean_header_label(text: str) -> str:
    """Normalize header token text (trim punctuation, collapse spaces)."""
    s = (text or "").strip()
    # normalize hyphens/dashes and strip trailing punctuation
    s = s.replace("–", "-").replace("—", "-")
    s = re.sub(r"[,:;]+$", "", s)         # "Jun-25," -> "Jun-25"
    s = re.sub(r"\s+", " ", s)
    return s

def _dedupe_headers_by_x(items: list[tuple[str, float]], min_gap: int = 50) -> list[tuple[str, float]]:
    """Keep headers left→right, dropping ones within min_gap px of the previous."""
    if not items:
        return []
    items = sorted(items, key=lambda z: z[1])
    kept = [items[0]]
    for name, pos in items[1:]:
        if (pos - kept[-1][1]) > min_gap:
            kept.append((name, pos))
    return kept

def _detect_headers_for_image(img):
    """
    Returns (col_labels: list[str], col_xy: dict[str,int]).
    Never returns None.
    """
    try:
        hdr_pos = _header_labels_from_image(img) or []  # list[(label, x)]
        labels  = [lab for lab, _ in hdr_pos]
        col_xy  = {lab: x for lab, x in hdr_pos}
        return labels, col_xy
    except Exception:
        return [], {}

def _detect_month_ytd_headers_from_lines(lines):
    """Return normalized column labels in left→right order using the first header band found."""
    if not lines:
        return []
    band = [r for r in lines[:min(15, len(lines))]]
    # pick tokens that look like a column header
    toks = []
    for r in band:
        for w in str(r["text"]).split():
            if MONTH_COL_RE.fullmatch(w) or YTD_COL_RE.fullmatch(w) or FORECAST_RE.fullmatch(" ".join([w])):
                toks.append((r["x"], w))
    if not toks:
        return []
    # unique by text (keep first x), then sort by x
    seen, uniq = {}, []
    for x, w in toks:
        key = w.lower()
        if key not in seen:
            seen[key] = x
            uniq.append((x, w))
    uniq.sort(key=lambda z: z[0])
    # Title-case a bit for output
    def norm(s): return re.sub(r"\s+", " ", s).strip().title()
    return [norm(w) for _, w in uniq]

def _detect_period_headers_xy(img_pil) -> list[tuple[str, int]]:
    """
    Return [(label, x_center_px), ...] for top-band period headers.
    Matches years, month-year (Jun-25), month names, YTD/TTM/etc.
    """
    arr = cv2.cvtColor(np.array(img_pil.convert("RGB")), cv2.COLOR_RGB2BGR)
    H, W = arr.shape[:2]
    band = arr[: max(1, int(0.32 * H)), :]  # top ~1/3
    df = pytesseract.image_to_data(
        band, output_type=pytesseract.Output.DATAFRAME,
        config="--psm 6 -c preserve_interword_spaces=1"
    )
    out = []
    try:
        df = df.dropna(subset=["text"])
        for _, r in df.iterrows():
            t = str(r["text"]).strip()
            if PERIOD_HEADER_RE.fullmatch(t):
                xc = int(r.get("left", 0)) + int(r.get("width", 0)) // 2
                out.append((t, xc))
    except Exception:
        pass
    seen = {}
    for lbl, xc in out:
        if lbl not in seen:
            seen[lbl] = xc
    return sorted(seen.items(), key=lambda z: z[1])

def _fallback_headers_from_lines(lines, max_cols_hint: int = 6):
    """
    Try to produce (labels, col_xy) from OCR line tokens.
    'lines' should be a list of word dicts with at least: {'text','x','y','w','h'}.
    Returns: (labels:List[str], col_xy:Dict[str,float])
    """
    words = []
    # accept either a flat list of word dicts, or a list of lines each with 'words'
    for item in (lines or []):
        if isinstance(item, dict) and "text" in item and "x" in item:
            words.append(item)
        elif isinstance(item, dict) and "words" in item:
            for w in item["words"]:
                if "text" in w and "x" in w:
                    words.append(w)

    # 1) look for explicit header-like words (months, YTD, Forecast...)
    hdr_candidates = []
    for w in words:
        txt = _clean_hdr_text(w.get("text", ""))
        if not txt:
            continue
        if _MONTH_COL_RE.search(txt) or _YTD_RE.search(txt) or _FORECAST_RE.search(txt):
            hdr_candidates.append((txt, float(w["x"])))

    # dedupe by x proximity
    def _dedupe_by_x(pairs, min_gap=24):
        pairs = sorted(pairs, key=lambda t: t[1])
        out = []
        for lab, x in pairs:
            if not out or abs(x - out[-1][1]) > min_gap:
                out.append((lab, x))
            else:
                # keep the earlier label (or replace with cleaner one)
                pass
        return out

    hdr_pos = _dedupe_by_x(hdr_candidates)

    # 2) If we still have too few headers, derive by clustering numeric columns
    if len(hdr_pos) < 2:
        num_xs = [float(w["x"]) for w in words if is_numeric_string(w.get("text", ""))]
        num_xs.sort()
        clusters = []
        gap = 35.0  # pixels between numeric columns
        for x in num_xs:
            if not clusters or (x - clusters[-1][-1]) > gap:
                clusters.append([x, x])  # start [leftmost, rightmost]
            else:
                clusters[-1][-1] = x     # extend rightmost
        centers = [median(c) if isinstance(c, list) else c for c in clusters]
        centers = sorted(centers)
        if 2 <= len(centers) <= 10:
            hdr_pos = [(f"Period{i+1}", cx) for i, cx in enumerate(centers[:max_cols_hint])]

    # 3) Build outputs
    labels = [_clean_hdr_text(lab) for (lab, _x) in hdr_pos]
    col_xy = {lab: float(x) for lab, x in hdr_pos}
    return labels, col_xy

def _find_header_bands(ocr_df: pd.DataFrame) -> list[dict]:
    """
    Scan the WHOLE page. A header band is a line that contains >=3 header tokens
    (e.g., 'Jun-25', 'May-25', 'June Forecast', 'YTD Actual', 'YTD Forecast').
    Returns list of {'y': mid_y, 'labels': [...], 'centers': [...]} ordered top->bottom.
    """
    bands = []

    # Group by printed line
    for (_, _), line in ocr_df.groupby(["block_num", "line_num"]):
        toks = [(t, float(cx)) for t, cx in zip(line["text"].tolist(), line["cx"].tolist())]

        # Collect single-token headers
        pairs: list[tuple[str, float]] = []
        for i, (t, x) in enumerate(toks):
            # 1) two-word joins first
            j = _stitch_two_word(toks, i, "ytd", "actual")
            if j: pairs.append(j); continue
            j = _stitch_two_word(toks, i, "ytd", "forecast")
            if j: pairs.append(j); continue
            j = _stitch_two_word(toks, i, "june", "forecast")
            if j: pairs.append(j); continue

            # 2) month-yy like "Jun-25" / "May-25"
            if _is_header_token(t):
                pairs.append((t, x))

        if len(pairs) < 3:
            continue

        # Deduplicate + sort by x
        seen, cleaned = set(), []
        for lab, x in sorted(pairs, key=lambda z: z[1]):
            key = re.sub(r"\s+", " ", lab.strip().title())
            if not key or key in seen:
                continue
            seen.add(key)
            cleaned.append((key, x))

        if len(cleaned) >= 3:
            labels  = [lab for lab, _ in cleaned]
            centers = [x   for _,   x in cleaned]
            y_mid   = float(line["cy"].mean())
            bands.append({"y": y_mid, "labels": labels, "centers": centers})

    # order by vertical position
    bands.sort(key=lambda b: b["y"])
    return bands

 

def _fix_header_ocr_token(tok: str) -> str:
    s = (tok or "").strip()
    if not s:
        return s
    for rx, fn in _HDR_OCR_FIXES:
        m = rx.match(s)
        if m:
            return fn(m)
    return s

def _fix_period_columns(df: pd.DataFrame, labels: list[str]) -> pd.DataFrame:
    """
    If DataFrame has Period1..PeriodK and we detected K headers, rename them.
    """
    if df is None or df.empty:
        return df
    period_cols = [c for c in df.columns if str(c).lower().startswith("period")]
    if period_cols and len(period_cols) == len(labels):
        mapping = {period_cols[i]: labels[i] for i in range(len(labels))}
        df = df.rename(columns=mapping)
    return df

def _get_headers_for_image(img):
    """
    Return (col_labels: List[str], col_xy: Dict[str, x_px]).
    Works with either variant of _header_labels_from_image:
      - returns (labels, col_xy)
      - OR returns list[(label, x)]
    """
    labels, col_xy = [], {}
    try:
        raw = _header_labels_from_image(img)
        # shape 1: (labels, col_xy)
        if isinstance(raw, tuple) and len(raw) == 2:
            labels, col_xy = raw
        else:
            # shape 2: list[(label, x)]
            pairs = raw or []
            try:
                pairs = _merge_and_clean_headers(pairs)  # if you added it
            except Exception:
                pass
            labels = [str(lab).strip() for lab, _x in pairs if str(lab).strip()]
            col_xy = {lab: x for lab, x in pairs if x is not None}
    except Exception:
        labels, col_xy = [], {}
    return labels, col_xy

def _header_labels_from_image(img):
    """
    Return header *pairs* [(label, x_center_px), ...] left→right.
    Primary: detect_period_columns_xy(img)
    Fallback: derive labels from lines and synthesize monotonically increasing x
    """
    pairs = []

    # 1) prefer your period detector that actually exists
    try:
        if callable(globals().get("detect_period_columns_xy")):
            pairs = detect_period_columns_xy(img) or []
    except Exception:
        pairs = []

    # 2) fallback: infer from OCR lines
    if not pairs:
        try:
            lines = ocr_lines(img)
            # if you have a line-based month/YTD header detector, use it
            fn = globals().get("_detect_month_ytd_headers_from_lines")
            labels = fn(lines) if callable(fn) else []
            if labels:
                # synthesize x positions so downstream code can keep working
                step = 120
                pairs = [(lab, (i + 1) * step) for i, lab in enumerate(labels)]
        except Exception:
            pairs = []

    return _merge_and_clean_headers(pairs)

def _is_header_label(cat: str) -> bool:
    s = re.sub(r"[^a-z0-9 ]+", " ", str(cat or "").lower())
    if not s.strip():
        return False
    # if the label has any digits, it's not a header banner
    if re.search(r"\d", s):
        return False
    words = [w for w in s.split() if w]
    monthish   = sum(w in MONTH_TOKENS for w in words)
    headerish  = sum(w in HEADERISH_WORDS for w in words)
    financeish = any(k in s for k in ["revenue","income","expense","profit","tax","aps"])
    # treat big grey bars like "MEDICAL PRODUCTS", "INDUSTRIAL PRODUCTS", "Revenue", etc. as headers
    return (monthish + headerish) >= 1 and not financeish

def _is_header_like_line(text: str) -> bool:
    """Return True if the OCR line is just a header band (months/YTD/forecast) or a page number."""
    t = re.sub(r"[,_()$•–—\-]+", " ", str(text or ""))
    t = re.sub(r"\s+", " ", t).strip()
    if not t:
        return True
    if MONTH_ONLY_LINE_RE.fullmatch(t):
        return True
    # page footer like "1.2"
    if re.fullmatch(r"\d+(?:\.\d+)?", t):
        return True
    return False

def _is_header_token(tok: str) -> bool:
    """Month-YY (Jun-25), YTD, Forecast single tokens."""
    s = str(s).strip()
    return bool(MONTH_COL_RE.search(s) or YTD_COL_RE.search(s) or FORECAST_RE.search(s))

def _is_month_token(s: str) -> bool:
    s = (s or "").strip().lower()
    return s in MONTH_TOKENS

def _looks_like_column_header(text: str) -> bool:
    t = (text or "").lower()
    month_hits = len(re.findall(r"(?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)", t))
    signal = ("forecast" in t) or ("ytd" in t) or (month_hits >= 2)
    return signal

def _looks_like_month_header(text: str, nums: list[float]) -> bool:
    """
    Heuristic: treat as header if line is mostly month/YTD words and small numbers (±0..200).
    This catches rows like: 'May   Jun   Y   -25%   12%'
    """
    t = (text or "").strip()
    if not t:
        return False
    low = re.sub(r"[^a-z0-9% ]+", " ", t.lower())
    words = [w for w in low.split() if w]
    if not words:
        return False

    monthish = sum(1 for w in words if _is_month_token(w) or w in {"y","ytd"})
    headerish = sum(1 for w in words if w in HEADER_HINTS)
    small_nums = sum(1 for n in nums if isinstance(n,(int,float)) and abs(n) <= 200)

    # If a line contains month/YTD tokens and no strong label words, and numbers are small → header
    return (monthish + headerish) >= 1 and small_nums >= 1 and not any(w in KEY_LABEL_HINTS for w in words)

def _merge_and_clean_headers(pairs):
    """
    Input: iterable of (label, x). Output: a cleaned list[(label, x)] left→right.
    - drops bad tuples
    - normalizes label text a bit
    - sorts by x
    """
    if not pairs:
        return []
    cleaned = []
    for p in pairs:
        if not isinstance(p, (tuple, list)) or len(p) != 2:
            continue
        lab, x = p
        if not lab:
            continue
        try:
            x = int(x)
        except Exception:
            continue
        lab = re.sub(r"\s+", " ", str(lab)).strip().rstrip(",;:")
        cleaned.append((lab, x))
    cleaned.sort(key=lambda z: z[1])
    return cleaned

def _norm_hdr_token(txt: str) -> str | None:
    """
    Normalize a single header token into a canonical form we care about.
    Returns None for words we do not want to treat as header labels alone.
    """
    if not txt:
        return None
    s = str(txt).strip()
    # strip trailing punctuation
    s = re.sub(r"[,:;.\u2013\u2014]+$", "", s)

    # month-year forms like "Jun-25" / "Jun 25"
    m = _HDR_MONYY.match(s)
    if m:
        return _canonical_month_yy(m.group(1), m.group(2))

    # single keywords we use only for stitching (do not keep alone except 'june' to join with 'forecast')
    w = _HDR_WORD.match(s)
    if w:
        return w.group(1).lower()

    # Sometimes Tesseract gives full month names; allow "June" only (to stitch with Forecast)
    if re.fullmatch(_MONTH_FULL, s, flags=re.I):
        return s.lower()

    return None  # everything else is noise for header building

def _stitch_two_word_header(line_df: pd.DataFrame, irow) -> str | None:
    """Join things like 'YTD' + 'Actual' or 'June' + 'Forecast'."""
    try:
        next_word_df = line_df[line_df.word_num == (irow.word_num + 1)]
        if next_word_df.empty:
            return None
        nxt = str(next_word_df.iloc[0]['text']).strip()
        cur = str(irow['text']).strip()
        key = (cur.lower(), nxt.lower())
        if key in {('ytd','actual'), ('ytd','forecast'), ('june','forecast'), ('jun','forecast')}:
            return f"{cur} {nxt.capitalize()}"
    except Exception:
        pass
    return None

_HDR_TOKEN = re.compile(
    r"""(?xi)
    (?: # month-year like May-25 / Jun-25
        (jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s*[-/]\s*\d{2,4}
    )
    |(?:ytd|actual|forecast)     # finance words
    |(?:jun[e]?)                 # stray "June" sometimes split from Forecast
    """
)
# ---- Header detection for months / YTD ----
MONTH_HEADER = re.compile(
    r"\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)"
    r"(?:[-/ ]?(?:\d{2}|\d{4}))?\b", re.I)
YTD_ACTUAL   = re.compile(r"\bYTD\s*Actual\b", re.I)
YTD_FORECAST = re.compile(r"\bYTD\s*Forecast\b", re.I)
FORECAST     = re.compile(r"\b(?:June|Jun)\s*Forecast\b", re.I)
# --- Add near your regex/constants ---
MONTH_TOKENS = {"jan","feb","mar","apr","may","jun","jul","aug","sep","sept","oct","nov","dec"}
HEADER_HINTS = {"ytd","forecast","plan","actual","budget","prior","vs","chg","change","%","percent"}

KEY_LABEL_HINTS = {"total", "revenue", "income", "loss", "expense", "profit", "tax", "cost", "aps"}
# Treat month/YTD/Forecast lines as header bands, not real categories
HEADERISH_WORDS = {"ytd", "forecast"}
try:
    MONTH_TOKENS  # already defined earlier in your file
except NameError:
    MONTH_TOKENS = {"jan","feb","mar","apr","may","jun","jul","aug","sep","sept","oct","nov","dec"}

# Common label constants (reduce typos across code)
LABEL_TOTAL_APS_INC_REVENUE           = "total aps, inc. revenue"
LABEL_OTHER_MEDICAL_CUSTOMERS         = "other medical customers"
LABEL_TOTAL_NONOP_GAINS_LOSSES        = "total non-operating gains (losses)"
LABEL_TOTAL_MEDICAL_PRODUCTS          = "total medical products"
LABEL_TOTAL_INDUSTRIAL_PRODUCTS       = "total industrial products"
LABEL_INTEREST_INCOME_NET             = "interest income, net"
LABEL_LOSS_ON_SALE_OF_ASSETS          = "loss on sale of assets"
LABEL_PROVISION_FOR_INCOME_TAXES      = "provision for income taxes"
LABEL_NET_INCOME_LOSS                 = "net income (loss)"
LABEL_MEDICAL_PRODUCTS_REVENUE        = "medical products revenue"

# Expected row sequence for the Products sheet when labels are missing
PRODUCTS_EXPECTED = [
    "customer #1", "customer #2", "customer #3", "customer #4", LABEL_OTHER_MEDICAL_CUSTOMERS,
    LABEL_TOTAL_MEDICAL_PRODUCTS,
    "matthew", "mark", "luke", "john", "peter",
    LABEL_TOTAL_INDUSTRIAL_PRODUCTS,
    LABEL_TOTAL_APS_INC_REVENUE,
]






import re
from typing import List, Tuple


def _stitch_two_word(tokens, i, a, b):
    """If tokens[i] == a and tokens[i+1] == b (case-insensitive) -> (label, cx)."""
    if i + 1 >= len(tokens):
        return None
    t1, x1 = tokens[i]
    t2, x2 = tokens[i+1]
    if t1.lower() == a and t2.lower() == b:
        return f"{t1} {t2}".title(), (x1 + x2) / 2.0
    return None


# --- Header post-processing ---
HEADER_PAIR_JOIN = {
    ("june", "forecast"):  "June Forecast",
    ("ytd",  "actual"):    "YTD Actual",
    ("ytd",  "forecast"):  "YTD Forecast",
}





def _is_rule_line(text: str) -> bool:
    # ignore pure underline/rule lines
    return bool(re.fullmatch(r"[_\-\u2014\=\s]+", (text or "").strip()))



# --- month/period header detection (add near other regexes) ---
MONTH_TOKEN = re.compile(
    r"\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*\s*[-/]\s*\d{2,4}\b",
    re.I,
)
_MONTHS = r"jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec"
MONTH_COL_RE   = re.compile(fr"(?i)\b(?:{_MONTHS})[a-z]*\s*-\s*\d{{2,4}}\b")   # e.g. Jun-25
YTD_COL_RE     = re.compile(r"(?i)\bYTD(?:\s+(?:Actual|Forecast))?\b")         # YTD, YTD Actual, YTD Forecast
FORECAST_RE    = re.compile(r"(?i)\b[A-Za-z]+\s+Forecast\b")                    # June Forecast, etc.

# A line that is *only* column headers (months/YTD/forecast terms)
MONTH_ONLY_LINE_RE = re.compile(
    fr"^(?:\s*(?:{_MONTHS})[a-z]*\s*-\s*\d{{2,4}}\s*|\s*YTD(?:\s+(?:Actual|Forecast))?\s*|\s*[A-Za-z]+\s+Forecast\s*)+$",
    re.I
)
# --- Header detection + cleanup ------------------------------------------------

# Month patterns (abbr and full)
_MONTH_ABBR = r"jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec"
_MONTH_FULL = r"january|february|march|april|may|june|july|august|september|october|november|december"

# e.g., "Jun-25", "May 25", "Jun–25", case-insensitive, commas allowed at end
_HDR_MONYY = re.compile(rf"^\s*({_MONTH_ABBR})[\s\-–—]?\s?(\d{{2}})\s*[,]?\s*$", re.I)
# single words we care about when they appear in headers
_HDR_WORD  = re.compile(r"^(ytd|actual|forecast|june|may)$", re.I)







PERIOD_HEADER_RE = re.compile(
    rf"""(?ix) ^
        (?:                                  # any one of:
            (?:19|20)\d{{2}}                 # 4-digit year: 2003, 2024
          | (?:{_MONTHS})[a-z]*              # month token
            \s*[-/ ]\s*
            (?:\d{{2}}|\d{{4}})              # YY or YYYY: Jun-25, May 2025
          | (?:YTD|TTM)                      # common period tags
        )
    $""")
# --- Header post-processing ---
MONTH_WORDS = {"jan","feb","mar","apr","may","jun","july","jul","aug","sep","sept","oct","nov","dec","june"}
TAIL_PUNCT = ",.;:"
# --- Header token cleaning/merging -------------------------------------------
HEADER_JOIN_GAP_PX = 28  # pixels: join tokens that are this close horizontally


# Heuristic fixes for common OCR glitches in header tokens
_HDR_OCR_FIXES = (
    (re.compile(r"^(?:s|d)un[-\s]?(\d{2,4})$", re.I), lambda m: f"Jun-{m.group(1)[-2:]}") ,
)


# Back-compat alias so existing code (e.g., parse_finance_lines, _unify_columns)
# that calls HDR_YEAR_RE.fullmatch(...) keeps working.
HDR_YEAR_RE = PERIOD_HEADER_RE
# Looser header matcher used by detectors (single token or stitched pairs)
PERIOD_LABEL_RE = re.compile(
    rf"""
    ^(?:{_MONTHS})[a-z]*\s*[-/ ]\s*(?:\d{{2}}|\d{{4}})$   # Jun-25 / May 2025
    |^ytd\s*(?:actual|forecast)$                           # YTD Actual / YTD Forecast
    |^(?:{_MONTHS})[a-z]*\s+forecast$                      # June Forecast
    """, re.I | re.X
)
NUM_TOKEN = re.compile(r"\(?-?\$?[\d,]+(?:\.\d+)?\)?(?:DR|%)?$", re.I)


# --- Normalization helpers (robust against punctuation/typos) ---
def _norm_key(s) -> str:
    # robust to None/NaN/numbers
    if s is None:
        return ""
    try:
        import pandas as pd
        if isinstance(s, float) and pd.isna(s):
            return ""
    except Exception:
        pass
    s = str(s)
    s = s.lower()
    s = re.sub(r"[^a-z0-9# ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

NORM_SYNONYMS = { _norm_key(k): _norm_key(v) for k, v in SYNONYMS.items() }
CANON_BY_NORM = { _norm_key(k): k for k in CANONICAL }
NORM_SYNONYMS.update({
    "yip forecast": "ytd forecast",
    "jun forecast": "june forecast",
    "jun-25": "june 25",
    "juneforecast": "june forecast",
})



def _norm_cat_for_match(s: str) -> str:
    k = _norm_key(s)
    return NORM_SYNONYMS.get(k, k)

# region [S5] Table Reconstruction & Postprocess

def _build_auto_ops_for_missing_totals(df: pd.DataFrame) -> dict:
    """
    Deterministic ops for two critical lines when components exist:
      - total non-operating gains (losses)
      - net income (loss)
      - total medical products / total industrial products
      - total revenue / total aps, inc. revenue
    """
    if df is None or df.empty or "Category" not in df.columns:
        return {"ops": []}

    ops = []
    cat = df["Category"].astype(str).map(lambda s: _canon_label(s).lower())
    have    = lambda k: bool(cat.eq(k).any())
    missing = lambda k: not have(k)
    val_cols = [c for c in df.columns if c != "Category" and not str(c).startswith("__")]

    # Total Non-Operating Gains (Losses)
    if (have(LABEL_INTEREST_INCOME_NET) or have(LABEL_LOSS_ON_SALE_OF_ASSETS) or have("donations (gift)")
        or have("interest expense") or have("other income") or have("other expense") or have("other income/(expense)")) \
        and missing(LABEL_TOTAL_NONOP_GAINS_LOSSES):
        comps = [LABEL_INTEREST_INCOME_NET, LABEL_LOSS_ON_SALE_OF_ASSETS, "donations (gift)",
                 "interest expense", "other income/(expense)", "other income", "other expense"]
        for col in val_cols:
            ops.append({
                "op": "add_and_calculate_row",
                "category": "total non-operating gains (losses)",
                "index": len(df),
                "col": col,
                "components": comps
            })

    # Net income (loss)
    if (have("operating income") or have(LABEL_TOTAL_NONOP_GAINS_LOSSES) or have(LABEL_PROVISION_FOR_INCOME_TAXES)) \
        and missing(LABEL_NET_INCOME_LOSS):
        comps = ["operating income", LABEL_TOTAL_NONOP_GAINS_LOSSES, LABEL_PROVISION_FOR_INCOME_TAXES]
        for col in val_cols:
            ops.append({
                "op": "add_and_calculate_row",
                "category": LABEL_NET_INCOME_LOSS,
                "index": len(df),
                "col": col,
                "components": comps
            })

    # Products sheet: totals from component lines
    # Medical products = customer #1..#4 + other medical customers
    medical_components = [
        "customer #1", "customer #2", "customer #3", "customer #4", LABEL_OTHER_MEDICAL_CUSTOMERS
    ]
    # Industrial products = matthew..peter
    industrial_components = ["matthew", "mark", "luke", "john", "peter"]

    have_any_med = any(have(_canon_label(x)) for x in medical_components)
    have_any_ind = any(have(_canon_label(x)) for x in industrial_components)

    if have_any_med and missing(LABEL_TOTAL_MEDICAL_PRODUCTS):
        for col in val_cols:
            ops.append({
                "op": "add_and_calculate_row",
                "category": LABEL_TOTAL_MEDICAL_PRODUCTS,
                "index": len(df),
                "col": col,
                "components": medical_components,
            })

    if have_any_ind and missing(LABEL_TOTAL_INDUSTRIAL_PRODUCTS):
        for col in val_cols:
            ops.append({
                "op": "add_and_calculate_row",
                "category": LABEL_TOTAL_INDUSTRIAL_PRODUCTS,
                "index": len(df),
                "col": col,
                "components": industrial_components,
            })

    # total revenue = total medical products + total industrial products
    if (have(LABEL_TOTAL_MEDICAL_PRODUCTS) or have_any_med) and (have(LABEL_TOTAL_INDUSTRIAL_PRODUCTS) or have_any_ind) and missing("total revenue"):
        for col in val_cols:
            ops.append({
                "op": "add_and_calculate_row",
                "category": "total revenue",
                "index": len(df),
                "col": col,
                "components": [LABEL_TOTAL_MEDICAL_PRODUCTS, LABEL_TOTAL_INDUSTRIAL_PRODUCTS],
            })

    # total aps, inc. revenue mirrors total revenue
    if missing(LABEL_TOTAL_APS_INC_REVENUE):
        for col in val_cols:
            ops.append({
                "op": "add_and_calculate_row",
                "category": LABEL_TOTAL_APS_INC_REVENUE,
                "index": len(df),
                "col": col,
                "components": ["total revenue"],
            })

    return {"ops": ops}

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
            vals = [ to_number(v) for v in g[c].tolist() ]
            nums = [v for v in vals if isinstance(v,(int,float))]
            row[c] = nums[0] if nums else None
        return pd.Series(row)

    out = (df.groupby("__norm", as_index=False, sort=False)
             .apply(chooser)
             .reset_index(drop=True))

    out = out.reindex(columns=["Category", *val_cols])
    return out

def _drop_year_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Removes any rows that look like just a year."""
    if df is None or df.empty or "Category" not in df.columns:
        return df
    
    # Identify rows where the "Category" value is a 4-digit number (a year)
    is_year_row = df["Category"].astype(str).str.match(r"^\d{4}$")
    
    return df[~is_year_row]

def _grid_is_bad(df: pd.DataFrame) -> bool:
    if df is None or df.empty or "Category" not in df.columns:
        return True
    val_cols = [c for c in df.columns if c != "Category"]
    if not val_cols:
        return True
    rows_with_nums = df[val_cols].notna().any(axis=1).sum()
    blank_cats = df["Category"].astype(str).str.strip().eq("").mean() if len(df) else 1.0
    # e.g., 1–2 numeric rows but no labels → junk
    if rows_with_nums <= 2 and blank_cats > 0:
        return True
    # mostly missing categories → junk
    if blank_cats >= 0.6:
        return True
    return False

def _unify_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Deduplicate columns and keep Category + up to MAX_VALUE_COLS value columns.
    Prefer month/YTD columns in a sane order for this sheet.
    """
    if df is None or df.empty:
        return df

    out = df.loc[:, ~df.columns.duplicated()]
    if "Category" not in out.columns:
        return out

    vals = [c for c in out.columns if c != "Category"]

    def _rank(c: str) -> int:
        s = str(c).lower()
        if "jun" in s and "forecast" not in s: return 0
        if "may" in s:                         return 1
        if "forecast" in s and "jun" in s:     return 2
        if "ytd actual" in s:                  return 3
        if "ytd forecast" in s or "ytd" in s:  return 4
        return 9

    keep = sorted(vals, key=lambda c: (_rank(c), vals.index(c)))[:MAX_VALUE_COLS]
    return out[["Category", *keep]]

def finalize(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans the final DataFrame by coercing types, dropping empty rows,
    and removing non-data header rows.
    """
    if df is None or df.empty:
        return pd.DataFrame()

    # 1) Coerce numerics
    df = coerce_numeric(df)

    # 2) Drop obvious non-data headers
    HEADERS_DROP = {"medical products revenue", "industrial products", "revenue"}
    try:
        df = df[~df['Category'].map(_canon_label).isin(HEADERS_DROP)]
    except Exception:
        pass

    # 3) Drop fully empty rows
    value_cols = [c for c in df.columns if c != 'Category']
    df = df.dropna(subset=['Category'] + value_cols, how='all')

    # 4) Remove helper columns
    df = df[[c for c in df.columns if not str(c).startswith('__')]]
    return df.reset_index(drop=True)

def order_like_statement(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["__rank"] = df["Category"].map(lambda s: ORDER_RANK.get(s, 99_999))
    if "__y" in df.columns:
        df = df.sort_values(["__rank", "__y"], ascending=[True, True])
    else:
        df = df.sort_values(["__rank", "Category"], ascending=[True, True])
    return df.drop(columns=["__rank"], errors="ignore")


# -------------------- Totals --------------------

# -------------------- Imaging --------------------

def prepare_image(img: Image.Image, max_long_side: int) -> Image.Image:
    """Resize the image so its longest side is at most max_long_side.
    Uses RESAMPLE if available. Returns a PIL Image.
    """
    try:
        w, h = img.size
        m = max(w, h)
        if m > max_long_side:
            r = float(max_long_side) / float(m)
            new_size = (max(1, int(round(w * r))), max(1, int(round(h * r))))
            try:
                return img.resize(new_size, RESAMPLE)
            except Exception:
                return img.resize(new_size)
        return img
    except Exception:
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

# region [S4] OCR & Layout Parsing

def _build_lines(df):
    # df has left, top, width, height, text
    df = df.sort_values(['top','left']).copy()
    if df.empty:
        return []
    # Use center-y for more stable line grouping
    df['cy'] = df['top'] + df['height']/2.0
    words = df.to_dict('records')
    median_h = float(np.median(df['height'])) or 12.0
    # Tighter threshold to avoid merging adjacent rows (totals vs first data row)
    thresh = max(6.0, 0.55 * median_h)

    lines = []
    cur = [words[0]]
    last_cy = float(words[0]['cy'])
    for w in words[1:]:
        cy = float(w.get('cy', w['top']))
        if (cy - last_cy) > thresh:
            lines.append(cur)
            cur = []
        cur.append(w)
        last_cy = cy
    if cur:
        lines.append(cur)
    return lines

def _group_rows_by_y(y_values: List[int] | List[float], tol: float = 12.0) -> List[int]:
    """Cluster Y centers into row bands by proximity and return band centers (ints).
    Uses a simple single-pass clustering with tolerance in pixels.
    """
    try:
        ys = sorted(int(round(float(y))) for y in (y_values or []))
    except Exception:
        return []
    if not ys:
        return []
    bands: list[list[int]] = [[ys[0]]]
    for y in ys[1:]:
        if abs(y - bands[-1][-1]) <= tol:
            bands[-1].append(y)
        else:
            bands.append([y])
    # use median of each band as canonical row y
    out: List[int] = []
    for b in bands:
        try:
            import statistics as _stats
            out.append(int(round(_stats.median(b))))
        except Exception:
            out.append(int(b[len(b)//2]))
    return out

def pdf_or_images_to_pages(paths: List[Path] | List[str], dpi: int = 300) -> List[Image.Image]:
    """Load a list of files (PDFs or images) and return a flat list of PIL Images.
    - PDFs are rendered via pdf2image.convert_from_bytes using POPPLER_BIN.
    - Images are opened via PIL; multi-frame images (e.g., TIFF) are expanded.
    """
    # Allow env override for DPI to help with faint digits (e.g., leading 5/1)
    try:
        dpi_env = int(os.environ.get('PDF2EX_DPI', '').strip() or 0)
        if dpi_env >= 200:
            dpi = dpi_env
    except Exception:
        pass
    out: List[Image.Image] = []
    if not paths:
        return out
    for fp in paths:
        try:
            p = Path(fp)
            ext = p.suffix.lower()
            if ext == ".pdf":
                data = p.read_bytes()
                try:
                    imgs = convert_from_bytes(data, dpi=dpi, poppler_path=str(POPPLER_BIN))
                except Exception:
                    imgs = convert_from_bytes(data, dpi=dpi)
                for im in imgs:
                    out.append(im.convert("RGB"))
            elif ext in {".png", ".jpg", ".jpeg", ".bmp", ".tif", ".tiff"}:
                im = Image.open(str(p))
                try:
                    # expand multi-frame images
                    from PIL import ImageSequence
                    frames = [f.convert("RGB") for f in ImageSequence.Iterator(im)]
                    out.extend(frames if frames else [im.convert("RGB")])
                except Exception:
                    out.append(im.convert("RGB"))
            else:
                # unknown extension; try opening as image
                im = Image.open(str(p))
                out.append(im.convert("RGB"))
        except Exception:
            continue
    return out

def detect_period_columns_xy(img: Image.Image) -> List[Tuple[str, int]]:
    """Detect period header columns (e.g., Jun-25, May-25, June Forecast, YTD Actual, YTD Forecast)
    and return [(label, x_center)] left-to-right. Uses line tokens for robustness.
    """
    try:
        lines = ocr_lines(img)
    except Exception:
        lines = []
    if not lines:
        return []

    pairs: list[tuple[str, float]] = []

    # helpers for stitching two-word headers
    def _stitch_pair(toks: List[dict], i: int, a: str, b: str, title_join: str) -> tuple[str, float] | None:
        if i + 1 >= len(toks):
            return None
        t1 = str(toks[i].get('t', '')).strip().lower()
        t2 = str(toks[i+1].get('t', '')).strip().lower()
        if t1 == a and t2 == b:
            x1 = float(toks[i].get('x', 0)); x2 = float(toks[i+1].get('x', 0))
            return (title_join, (x1 + x2) / 2.0)
        return None

    # scan lines in the upper half first
    H = img.size[1]
    YEAR_RE = re.compile(r'^(19|20)\d{2}$')
    for ln in lines:
        y = int(ln.get('y', 0))
        if y > H * 0.6:
            continue
        toks = ln.get('tokens') or []
        if not toks:
            continue
        # collect header tokens from this line
        local: list[tuple[str, float]] = []
        for i, t in enumerate(toks):
            s = str(t.get('t', '')).strip()
            s_clean = re.sub(r"[,:]+$", "", s)
            # month-yy like Jun-25 / May-25
            if _HDR_MONYY.fullmatch(s_clean):
                local.append((s_clean, float(t.get('x', 0))))
            # year-only headers like 2003 / 2004
            if YEAR_RE.fullmatch(s_clean):
                local.append((s_clean, float(t.get('x', 0))))
                continue
                continue
            # Stitch split month + year tokens (e.g., "May" "25") into "May-25"
            try:
                mon = s_clean.lower()
                if re.fullmatch(rf"({_MONTH_ABBR}|{_MONTH_FULL})", mon, re.I) and i + 1 < len(toks):
                    t2 = str(toks[i+1].get('t','')).strip()
                    if re.fullmatch(r"\d{2,4}", t2):
                        yy = t2[-2:]
                        x1 = float(t.get('x', 0)); x2 = float(toks[i+1].get('x', 0))
                        local.append((f"{s_clean}-{yy}", (x1 + x2) / 2.0))
                        continue
            except Exception:
                pass
            # stitch YTD Actual / YTD Forecast / June Forecast
            joined = _stitch_pair(toks, i, 'ytd', 'actual', 'YTD Actual')
            if joined:
                local.append(joined); continue
            joined = _stitch_pair(toks, i, 'ytd', 'forecast', 'YTD Forecast')
            if joined:
                local.append(joined); continue
            joined = _stitch_pair(toks, i, 'june', 'forecast', 'June Forecast')
            if joined:
                local.append(joined); continue

        # If we found at least 2 headers on this line, treat it as the header band
        if len(local) >= 2:
            # dedupe by normalized label, keep first x
            seen: dict[str, float] = {}
            for lab, x in local:
                key = _canon_col_name_v3(str(lab))
                if key and key not in seen:
                    seen[key] = float(x)
            pairs = [(k, v) for k, v in seen.items()]
            break

    if not pairs:
        return []
    pairs.sort(key=lambda z: z[1])
    # convert x to int and label cleaned
    cleaned: List[Tuple[str, int]] = []
    for lab, x in pairs:
        cleaned.append((_canon_col_name_v3(str(lab)), int(round(float(x)))))
    return cleaned

def ocr_lines(img: Image.Image) -> List[Dict]:
    """
    Line-level OCR with per-token positions.
    Robust geometric line grouping by vertical proximity to avoid Tesseract's
    sometimes-messy internal line segmentation on indented layouts.
    """
    config_str = "--psm 11 --oem 1 -c preserve_interword_spaces=1"
    try:
        arr = cv2.cvtColor(np.array(img), cv2.COLOR_RGB2BGR)
    except Exception:
        arr = np.array(img)

    df = pytesseract.image_to_data(arr, config=config_str, output_type=pytesseract.Output.DATAFRAME)
    if df is None or df.empty:
        return []

    try:
        df.dropna(subset=['text'], inplace=True)
        df['text'] = df['text'].astype(str).str.strip()
        df = df[df['text'] != '']
    except Exception:
        return []

    if df.empty:
        return []

    # Sort top-to-bottom, then left-to-right
    df = df.sort_values(['top', 'left']).reset_index(drop=True)

    # Dynamic threshold based on median token height
    try:
        median_height = float(df['height'].median())
    except Exception:
        median_height = 12.0
    line_break_threshold = max(6.0, 0.7 * median_height)

    lines_raw: list[list[dict]] = []
    current: list[dict] = []

    # Helper to convert df row -> plain dict of needed fields
    def _row_to_token(row) -> dict:
        return {
            'text': str(row.get('text', '')).strip(),
            'left': int(row.get('left', 0)),
            'top': int(row.get('top', 0)),
            'width': int(row.get('width', 0)),
            'height': int(row.get('height', 0)),
        }

    if not df.empty:
        current.append(_row_to_token(df.iloc[0]))

    for i in range(1, len(df)):
        prev = current[-1]
        cur_row = _row_to_token(df.iloc[i])
        try:
            prev_cy = prev['top'] + prev.get('height', 0) / 2.0
            cur_cy  = cur_row['top'] + cur_row.get('height', 0) / 2.0
            vgap = abs(cur_cy - prev_cy)
        except Exception:
            vgap = line_break_threshold + 1

        if vgap > line_break_threshold:
            if current:
                lines_raw.append(current)
            current = []
        current.append(cur_row)

    if current:
        lines_raw.append(current)

    # Convert to final structure
    out_lines: List[Dict] = []
    for group in lines_raw:
        if not group:
            continue
        group = sorted(group, key=lambda r: r['left'])
        toks = [
            {
                't': r['text'],
                'x': int(r['left'] + r.get('width', 0) / 2),
                'y': int(r['top']),
                'w': int(r.get('width', 0)),
            }
            for r in group if r.get('text')
        ]
        if not toks:
            continue
        text = " ".join(z['t'] for z in toks)
        y_pos = min(r['top'] for r in group)
        x_pos = min(r['left'] for r in group)
        out_lines.append({'text': text, 'conf': 0.0, 'y': int(y_pos), 'x': int(x_pos), 'tokens': toks})

    out_lines.sort(key=lambda r: (r['y'], r['x']))
    return out_lines

def _write_page_debug(img: Image.Image, page_num: int) -> None:
    """Best‑effort page diagnostics written to DEBUG_DIR regardless of parser path.
    Writes:
      - pXX_input.png: the image we are about to parse
      - pXX_layout.png: overlay with column and row anchors
      - pXX_headers.json: header pairs [(label, x)]
      - pXX_lines.json: first ~120 OCR lines with tokens (trimmed)
    """
    try:
        DEBUG_DIR.mkdir(parents=True, exist_ok=True)
    except Exception:
        return
    # Save the input image we're using for parsing
    try:
        img.convert("RGB").save(DEBUG_DIR / f"p{page_num:02d}_input.png")
    except Exception:
        pass
    # Overlay (columns+rows)
    try:
        _rewrite_layout_overlay(img, page_num)
    except Exception:
        pass
    # Headers
    try:
        pairs = detect_period_columns_xy(img) or []
        (DEBUG_DIR / f"p{page_num:02d}_headers.json").write_text(
            json.dumps(pairs, indent=2), encoding="utf-8")
    except Exception:
        pass
    # OCR lines (trimmed)
    try:
        lines = ocr_lines(img) or []
        # Trim to avoid giant files
        trimmed = []
        for i, ln in enumerate(lines[:120]):
            toks = ln.get("tokens") or []
            if len(toks) > 25:
                toks = toks[:25]
            trimmed.append({
                "text": ln.get("text", ""),
                "y": int(ln.get("y", 0)),
                "x": int(ln.get("x", 0)),
                "conf": float(ln.get("conf", 0)),
                "tokens": toks,
            })
        (DEBUG_DIR / f"p{page_num:02d}_lines.json").write_text(
            json.dumps(trimmed, indent=2), encoding="utf-8")
    except Exception:
        pass

def parse_by_cell_ocr(img: Image.Image, page_num: int = 1) -> Optional[pd.DataFrame]:
    """
    Final robust parser. Establish row grid from numeric tokens, then attach
    left labels to that grid. Wider, safer ROIs for cell OCR. Keeps totals.
    """
    # 1) Column anchors from headers (with robust fallbacks)
    # Two-pass headers: detect on the original image, but extract values on a
    # slightly upscaled copy to preserve thin leading digits. Anchors are scaled.
    img_hdr = img
    scale_env = os.environ.get('PDF2EX_VAL_SCALE', '').strip()
    try:
        VAL_SCALE = float(scale_env) if scale_env else 1.7
    except Exception:
        VAL_SCALE = 1.4
    VAL_SCALE = max(1.0, min(2.0, VAL_SCALE))

    # Detect headers on header image
    try:
        hdr_pairs = detect_period_columns_xy(img_hdr)
    except Exception:
        hdr_pairs = []
    col_names: List[str] = []
    col_anchors: Dict[str, int] = {}
    if hdr_pairs and len(hdr_pairs) >= 3:
        col_names = [_canon_col_name_v3(lab) for lab, _ in hdr_pairs]
        # scale anchors for the value image (which may be upscaled below)
        if VAL_SCALE != 1.0:
            col_anchors = {name: int(round(x * VAL_SCALE)) for name, (_, x) in zip(col_names, hdr_pairs)}
        else:
            col_anchors = {name: x for name, (_, x) in zip(col_names, hdr_pairs)}
    else:
        # Fallback 1: infer headers from lines
        try:
            lines_l = ocr_lines(img)
            labels, col_xy = _fallback_headers_from_lines(lines_l, max_cols_hint=5)
        except Exception:
            labels, col_xy = [], {}
        if labels and len(labels) >= 3:
            col_names = [_canon_col_name_v3(l) for l in labels]
            ordered = sorted([(nm, int(col_xy[nm])) for nm in labels if nm in col_xy], key=lambda z: z[1])
            col_anchors = { _canon_col_name_v3(nm): int(x) for nm, x in ordered }
        else:
            # Fallback 2: cluster numeric-token x positions into generic Period columns
            try:
                lines_l = ocr_lines(img)
                xs = []
                for ln in (lines_l or []):
                    for t in (ln.get('tokens') or []):
                        s = str(t.get('t','')).strip()
                        if is_numeric_string(s):
                            xs.append(int(t.get('x',0)))
                xs.sort()
                clusters: List[List[int]] = []
                gap = 35
                for x in xs:
                    if not clusters or (x - clusters[-1][-1]) > gap:
                        clusters.append([x])
                    else:
                        clusters[-1].append(x)
                centers = []
                import statistics as _stats
                for c in clusters:
                    try:
                        centers.append(int(round(_stats.median(c))))
                    except Exception:
                        centers.append(int(round(sum(c)/len(c))))
                centers = sorted(centers)
                if len(centers) >= 3:
                    col_names = [f"Period{i+1}" for i in range(len(centers))]
                    col_anchors = { col_names[i]: centers[i] for i in range(len(centers)) }
                else:
                    return None
            except Exception:
                return None

    # Debug: write detected headers
    try:
        DEBUG_DIR.mkdir(parents=True, exist_ok=True)
        pairs_dbg = [(nm, int(x)) for nm, x in col_anchors.items()]
        pairs_dbg = sorted(pairs_dbg, key=lambda z: z[1])
        (DEBUG_DIR / f"p{page_num:02d}_headers.json").write_text(json.dumps(pairs_dbg, indent=2), encoding='utf-8')
    except Exception:
        pass

    # 2) Unified numeric row grid
    # Build the value image (potentially upscaled) AFTER header detection
    if VAL_SCALE != 1.0:
        try:
            w, h = img.size
            img = img.resize((int(round(w * VAL_SCALE)), int(round(h * VAL_SCALE))), RESAMPLE)
        except Exception:
            pass
    try:
        lines = ocr_lines(img)
    except Exception:
        lines = []
    H = img.size[1]
    numeric_ys: list[int] = []
    for ln in (lines or []):
        y_pos = int(ln.get('y', 0))
        # skip deep footer noise only
        if y_pos > int(0.96 * H):
            continue
        toks = ln.get('tokens') or []
        if any(is_numeric_string(str(t.get('t',''))) for t in toks):
            numeric_ys.append(y_pos)
    if not numeric_ys:
        return None
    row_y_anchors = _group_rows_by_y(numeric_ys, tol=10)

    # 3) Attach left labels to the fixed row grid
    row_anchors: list[dict] = []
    first_col_x = min(col_anchors.values()) if col_anchors else img.size[0]
    left_labels: list[dict] = []
    for ln in (lines or []):
        y_pos = int(ln.get('y', 0))
        label_toks = [t for t in (ln.get('tokens') or [])
                      if int(t.get('x', 9_999_999)) < int(first_col_x) - 15 and not is_numeric_string(str(t.get('t','')))]
        if not label_toks:
            continue
        txt = ' '.join(str(t.get('t','')) for t in label_toks).strip()
        if txt:
            left_labels.append({'y': y_pos, 'text': txt})
    for y in row_y_anchors:
        best_label, best_d = '', 1e9
        for lab in left_labels:
            d = abs(int(lab['y']) - int(y))
            if d < best_d and d < 12:
                best_d = d
                best_label = lab['text']
        row_anchors.append({'y': int(y), 'Category': _canon_label(best_label)})

    # Diagnostics
    try:
        DEBUG_DIR.mkdir(parents=True, exist_ok=True)
        (DEBUG_DIR / f"p{page_num:02d}_row_anchors.json").write_text(json.dumps(row_anchors, indent=2), encoding='utf-8')
    except Exception:
        pass

    # 4) Targeted OCR per cell with wider ROIs
    out_rows: list[dict] = []
    img_arr = np.array(img.convert('L'))
    H, W = img_arr.shape[:2]

    sorted_cols = sorted(col_anchors.items(), key=lambda kv: kv[1])
    safe_half: dict[str, float] = {}
    for i, (name, x) in enumerate(sorted_cols):
        left_gap = x - sorted_cols[i-1][1] if i > 0 else 120
        right_gap = sorted_cols[i+1][1] - x if i < len(sorted_cols)-1 else 120
        safe_half[name] = max(30.0, min(left_gap, right_gap) / 2.0 - 5)

    # Build per-column boundaries for token snapping
    anchors = [x for _, x in sorted_cols]
    bounds: list[float] = []
    for i in range(len(anchors)+1):
        if i == 0:
            bounds.append(max(0.0, anchors[0] - max(20.0, (anchors[1]-anchors[0])/2.0)))
        elif i == len(anchors):
            bounds.append(min(float(W), anchors[-1] + max(20.0, (anchors[-1]-anchors[-2])/2.0)))
        else:
            bounds.append((anchors[i-1] + anchors[i]) / 2.0)

    # Row boundaries from row_y_anchors
    ybounds: list[float] = []
    for i in range(len(row_y_anchors)+1):
        if i == 0:
            ybounds.append(max(0.0, row_y_anchors[0] - 12.0))
        elif i == len(row_y_anchors):
            ybounds.append(min(float(H), row_y_anchors[-1] + 14.0))
        else:
            ybounds.append((row_y_anchors[i-1] + row_y_anchors[i]) / 2.0)

    # Extract numeric tokens once and filter out header-band echoes
    header_ys = []
    for ln in (lines or []):
        txt = str(ln.get('text','')).lower()
        if any(k in txt for k in ['jun','may','ytd','forecast']):
            header_ys.append(int(ln.get('y',0)))
    tokens = []
    try:
        df_data = pytesseract.image_to_data(img, output_type=Output.DATAFRAME,
                                            config="--psm 6 --oem 1 -c preserve_interword_spaces=1")
        if df_data is not None and not df_data.empty:
            df2 = df_data.dropna(subset=['text']).copy()
            for _, r in df2.iterrows():
                s = str(r.get('text','')).strip()
                v = to_number(s)
                if not isinstance(v,(int,float)):
                    continue
                x = int(r.get('left',0)) + int(r.get('width',0))//2
                y = int(r.get('top',0))
                w = int(r.get('width',0))
                if ALIGN_NUMS_RIGHT:
                    x = x + max(0, w//2)
                if any(abs(y-hy) <= 8 for hy in header_ys):
                    continue
                if y >= int(0.96*H):
                    continue
                tokens.append({'x': x, 'y': y, 'val': v, 'digits': len(re.sub(r'\D','', s))})
    except Exception:
        tokens = []

    index_by_name = {name: idx for idx, (name, _) in enumerate(sorted_cols)}

    # Column right-anchor calibration (median right-edge per column)
    rx_by_name: Dict[str, float] = {}
    try:
        for name, cx in sorted_cols:
            i = index_by_name.get(name)
            if i is None:
                continue
            lb = bounds[i] - 8.0
            ub = bounds[i+1] + 8.0
            xs = [t['x'] for t in tokens if lb <= t['x'] < ub]
            if xs:
                import statistics as _stats
                try:
                    rx_by_name[name] = float(_stats.median(xs))
                except Exception:
                    rx_by_name[name] = float(sum(xs)/len(xs))
            else:
                rx_by_name[name] = float(cx)
    except Exception:
        rx_by_name = {name: float(x) for name, x in sorted_cols}

    # Debug: save calibrated right anchors
    try:
        DEBUG_DIR.mkdir(parents=True, exist_ok=True)
        (DEBUG_DIR / f"p{page_num:02d}_col_right_anchors.json").write_text(
            json.dumps(rx_by_name, indent=2), encoding='utf-8')
    except Exception:
        pass

    cells_dir = DEBUG_DIR / 'cells'
    try:
        cells_dir.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass

    debug_cells: list[dict] = []

    for ra in row_anchors:
        row = {'Category': ra['Category'], '__y': int(ra['y'])}
        y_anchor = int(ra['y'])
        has_val = False
        for name, x_anchor in col_anchors.items():
            hw = int(safe_half.get(name, 45))
            name_low = str(name).lower()
            extra_w = 18 if 'forecast' in name_low else (12 if 'ytd' in name_low else 0)
            x1 = max(0, int(x_anchor - hw - extra_w))
            x2 = min(W, int(x_anchor + hw + extra_w))
            y1 = max(0, int(y_anchor - 10))
            y2 = min(H, int(y_anchor + 20))
            if x2 <= x1 or y2 <= y1:
                row[name] = None
                continue
            # Token-first candidate fill within col bounds and row band
            ci = index_by_name.get(name, None)
            cell_value = None
            cell_text = ''
            if ci is not None:
                lb = bounds[ci]; ub = bounds[ci+1]
                # allow a little horizontal slack, more for YTD columns
                name_low = str(name).lower()
                slack = 8.0
                if 'ytd' in name_low:
                    slack = 16.0
                rx = rx_by_name.get(name, float(x_anchor))
                cand = [t for t in tokens if (lb - slack <= t['x'] < ub + slack) and (y1 <= t['y'] < y2)]
                # Assign tokens to the nearest column by calibrated right anchors
                def _nearest_col_name(tx):
                    best = None; bestd = 1e18
                    for nm, _cx in sorted_cols:
                        rxx = rx_by_name.get(nm, float(_cx))
                        d = abs(float(tx['x']) - float(rxx))
                        if d < bestd:
                            bestd = d; best = nm
                    return best
                cand = [t for t in cand if _nearest_col_name(t) == name]
                # Prefer realistic digit counts: YTD columns tend to be large (>=5 digits)
                min_digits = 5 if 'ytd' in name_low else 3
                cand = [t for t in cand if t['digits'] >= min_digits]
                if cand:
                    y0 = (y1 + y2)/2.0
                    cand.sort(key=lambda t: (-t['digits'], abs(t['y']-y0), abs(t['x']-rx)))
                    token_val = cand[0]['val']
                    cell_value = token_val
                    # Guard against clipped leading digit (e.g., 520,219 -> 20,219)
                    # Validate against a slightly widened ROI using right-edge strategy.
                    try:
                        roi = img_arr[y1:y2, max(0, x1-6):min(W, x2+6)]
                        local_target = float(int(x_anchor) - int(max(0, x1-6)))
                        gv = _best_num_from_roi(roi, local_target, strategy='right')
                        def _nd(v):
                            try:
                                return len(str(int(abs(v))))
                            except Exception:
                                return 0
                        if isinstance(gv, (int, float)) and _nd(gv) > _nd(token_val):
                            # If ROI number ends with the token's digits, prefer ROI (likely lost a leading digit)
                            tvs = str(int(abs(token_val)))
                            gvs = str(int(abs(gv)))
                            if gvs.endswith(tvs):
                                cell_value = gv
                    except Exception:
                        pass
            # ROI fallback if no token
            if cell_value is None:
                roi = img_arr[y1:y2, x1:x2]
                try:
                    cell_text = pytesseract.image_to_string(roi, config=NUM_TESS_CFG).strip()
                except Exception:
                    cell_text = ''
                if not _is_dash_only(cell_text):
                    cell_value = to_number(cell_text)
                if cell_value is None:
                    try:
                        t2 = pytesseract.image_to_string(roi, config='--oem 1 --psm 7').strip()
                        if not _is_dash_only(t2):
                            cell_value = to_number(t2)
                    except Exception:
                        pass
                if cell_value is None:
                    try:
                        t3 = pytesseract.image_to_string(roi, config='--oem 1 --psm 8').strip()
                        if not _is_dash_only(t3):
                            cell_value = to_number(t3)
                    except Exception:
                        pass
                if cell_value is None:
                    try:
                        import cv2 as _cv
                        _, rbin = _cv.threshold(roi, 0, 255, _cv.THRESH_BINARY + _cv.THRESH_OTSU)
                        t4 = pytesseract.image_to_string(rbin, config=NUM_TESS_CFG).strip()
                        if not _is_dash_only(t4):
                            cell_value = to_number(t4)
                    except Exception:
                        pass
                if cell_value is None:
                    # Light dilation can recover thin/missing leading strokes
                    try:
                        import cv2 as _cv
                        kernel = _cv.getStructuringElement(_cv.MORPH_RECT, (2, 2))
                        rbin2 = _cv.dilate(rbin, kernel, iterations=1)
                        t5 = pytesseract.image_to_string(rbin2, config=NUM_TESS_CFG).strip()
                        if not _is_dash_only(t5):
                            cell_value = to_number(t5)
                    except Exception:
                        pass
                # Extra attempts: widen ROI to the left; alt taller band; geometry-aware pick
                if cell_value is None:
                    try:
                        rx1 = max(0, x1 - 32); rx2 = x2
                        y0 = int((y1 + y2) / 2)
                        rroi = img_arr[y1:y2, rx1:rx2]
                        rroi_alt = img_arr[max(0, y0-16):min(H, y0+18), rx1:rx2]
                        def _ocr_roi(a):
                            try:
                                return pytesseract.image_to_string(a, config=NUM_TESS_CFG).strip()
                            except Exception:
                                return ''
                        rtxt = _ocr_roi(rroi)
                        if not _is_dash_only(rtxt):
                            cell_value = to_number(rtxt)
                        if cell_value is None:
                            rtxtA = _ocr_roi(rroi_alt)
                            if not _is_dash_only(rtxtA):
                                cell_value = to_number(rtxtA)
                        if cell_value is None:
                            gv2 = _best_num_from_roi(rroi, float(x_anchor - rx1), strategy='right')
                            if isinstance(gv2, (int, float)):
                                cell_value = gv2
                    except Exception:
                        pass
                # Geometry-aware number: prefer if it has more digits
                try:
                    local_target = float(int(x_anchor) - int(x1))
                    gv = _best_num_from_roi(roi, local_target, strategy='right')
                    if isinstance(gv,(int,float)):
                        if cell_value is None:
                            cell_value = gv
                        else:
                            def _nd(v):
                                try:
                                    return len(str(int(abs(v))))
                                except Exception:
                                    return 0
                            if _nd(gv) > _nd(cell_value):
                                cell_value = gv
                except Exception:
                    pass
            if isinstance(cell_value, (int,float)):
                has_val = True
            row[name] = cell_value
            # Save per-cell crops and text
            try:
                cat_safe = str(ra['Category']).replace('#','').replace(' ','_')[:20]
                from PIL import Image as _Img
                img_path = cells_dir / f"r_{cat_safe}_c_{name}.png"
                txt_path = cells_dir / f"r_{cat_safe}_c_{name}.txt"
                _Img.fromarray(roi).save(img_path)
                txt_path.write_text(f"Raw OCR: '{cell_text}'\nParsed: {cell_value}", encoding='utf-8')
            except Exception:
                pass
            debug_cells.append({'page': page_num,'category': ra['Category'],'y': y_anchor,'col': name,'x_anchor': float(x_anchor),'x1': x1,'y1': y1,'x2': x2,'y2': y2,'text': cell_text,'value': cell_value})
        # keep rows that contain numbers OR are known section headers
        is_known_header = _canon_label(row.get('Category','')) in HEADERS_KEEP
        if has_val or is_known_header:
            out_rows.append(row)

    # Always write the cell OCR CSV for diagnostics
    try:
        import csv
        with open(DEBUG_DIR / f"p{page_num:02d}_cell_ocr.csv", 'w', newline='', encoding='utf-8') as f:
            w = csv.DictWriter(f, fieldnames=['page','category','y','col','x_anchor','x1','y1','x2','y2','text','value'])
            w.writeheader()
            for r in debug_cells:
                w.writerow(r)
        with open(DEBUG_DIR / f"p{page_num:02d}_assignments_colaware.csv", 'w', newline='', encoding='utf-8') as f2:
            w2 = csv.DictWriter(f2, fieldnames=['category','col','text','x','y','reason'])
            w2.writeheader()
            for r in debug_cells:
                w2.writerow({'category': r['category'], 'col': r['col'], 'text': r['text'], 'x': r['x_anchor'], 'y': r['y'], 'reason': 'cell'})
    except Exception:
        pass

    if not out_rows:
        return None
    # Build DataFrame, keep __y for a final repair pass using global tokens
    df = pd.DataFrame(out_rows).sort_values('__y')
    try:
        # Final repair: fill stubborn blanks by snapping nearest global token in the column
        if tokens:
            index_by_name = {name: idx for idx, (name, _) in enumerate(sorted_cols)}
            for ridx, row in df.iterrows():
                y_anchor = int(row.get('__y', 0))
                for name in col_names:
                    if name not in df.columns:
                        continue
                    val = row.get(name, None)
                    # Only attempt for Forecast/YTD Forecast and missing/zero values
                    if not (('forecast' in name.lower()) or ('ytd' in name.lower())):
                        continue
                    try:
                        is_missing = (val is None) or (pd.isna(val)) or (str(val).strip()=='' ) or (float(val)==0.0)
                    except Exception:
                        is_missing = True
                    if not is_missing:
                        continue
                    ci = index_by_name.get(name)
                    if ci is None:
                        continue
                    lb = bounds[ci]; ub = bounds[ci+1]
                    slack = 16.0
                    ytol = 22.0 if 'ytd' in name.lower() else 16.0
                    rx = rx_by_name.get(name, float(sorted_cols[ci][1]))
                    # find candidate tokens within column bounds and near row y
                    cand = [t for t in tokens if (lb - slack <= t['x'] < ub + slack) and (abs(int(t['y']) - y_anchor) <= ytol)]
                    # Keep tokens whose nearest column (by calibrated right anchors) is this column
                    def _nearest_col_name(tx):
                        best = None; bestd = 1e18
                        for nm, _cx in sorted_cols:
                            rxx = rx_by_name.get(nm, float(_cx))
                            d = abs(float(tx['x']) - float(rxx))
                            if d < bestd:
                                bestd = d; best = nm
                        return best
                    cand = [t for t in cand if _nearest_col_name(t) == name]
                    # Prefer higher digit counts and proximity to right anchor
                    if cand:
                        cand.sort(key=lambda t: (-t['digits'], abs(int(t['y'])-y_anchor), abs(float(t['x'])-rx)))
                        df.at[ridx, name] = cand[0]['val']
    except Exception:
        pass
    # Drop helper column and finalize ordering
    df = df.drop(columns=['__y'])
    for c in col_names:
        if c not in df.columns:
            df[c] = pd.NA
    df = df.reindex(columns=['Category', *col_names])
    return df


def process_image(img: Image.Image, page_num: int = 1) -> Optional[pd.DataFrame]:
    """
    Simplified: preprocess for OCR and run the definitive cell-based parser.
    """
    img = prepare_image(img, MAX_LONG_SIDE)
    processed_img = _preprocess_for_ocr(img)

    # Attempt: definitive cell parser on preprocessed, then fallback to original/underline-removed
    try:
        df = parse_by_cell_ocr(processed_img, page_num=page_num)
        if df is None or df.empty:
            df = parse_by_cell_ocr(img, page_num=page_num)
        if df is None or df.empty:
            df = parse_by_cell_ocr(remove_underlines(img), page_num=page_num)
        if df is None or df.empty:
            print(f"Page {page_num}: No table found by the parser.")
            try:
                DEBUG_DIR.mkdir(parents=True, exist_ok=True)
                processed_img.save(DEBUG_DIR / f"p{page_num:02d}_preprocessed.png")
            except Exception:
                pass
            return None
        try:
            DEBUG_DIR.mkdir(parents=True, exist_ok=True)
            processed_img.save(DEBUG_DIR / f"p{page_num:02d}_preprocessed.png")
            df.to_csv(DEBUG_DIR / f"p{page_num:02d}_table.csv", index=False, encoding='utf-8-sig')
        except Exception:
            pass
        return df
    except Exception as e:
        print(f"Error processing page {page_num}: {e}")
        try:
            DEBUG_DIR.mkdir(parents=True, exist_ok=True)
            processed_img.save(DEBUG_DIR / f"p{page_num:02d}_preprocessed.png")
        except Exception:
            pass
        return None

    # Heuristic: if we clearly see 5 period headers but almost no left-side labels,
    # jump straight to products-mode reconstruction which does not require labels.
    def _should_products_mode(image: Image.Image) -> bool:
        try:
            pairs = detect_period_columns_xy(image)
        except Exception:
            pairs = []
        if not pairs or len(pairs) < 5:
            return False
        first_x = min(x for _lab, x in pairs)
        L = ocr_lines(image)
        left_words = 0
        for ln in L:
            for t in (ln.get('tokens') or []):
                txt = str(t.get('t','')).strip()
                if txt and (t.get('x', 0) < (first_x - 18)) and re.search(r'[A-Za-z#]', txt):
                    left_words += 1
                    if left_words >= 4:
                        return False
        return True

    force = os.environ.get('PDF2EX_FORCE', '').strip().lower() or None
    if _should_products_mode(img):
        # Always include layout_v4 as a candidate too — often more reliable for this layout
        try:
            df_layout = parse_by_layout_v4(img_proc, page_num=page_num)
            candidates.append(("layout_v4", df_layout))
        except Exception:
            candidates.append(("layout_v4", None))
        df_a = _parse_products_by_lines(img)
        candidates.append(("products_lines", df_a))
        try:
            df_b = _parse_products_by_centers(img)
            candidates.append(("products_centers", df_b))
        except Exception:
            pass
    else:
        # Attempt 1: value-first layout (optional; stubbed if not present)
        try:
            df_a = parse_by_layout_v4(img_proc, page_num=page_num)
        except Exception:
            df_a = None
        candidates.append(("layout_v4", df_a))

    # Fallback 1: multi-band header parser if the grid looks weak
    def _too_small(d: Optional[pd.DataFrame]) -> bool:
        if d is None or not isinstance(d, pd.DataFrame) or d.empty:
            return True
        val_cols = [c for c in d.columns if c != "Category"]
        return (len(val_cols) < 3) or (len(d) < 5)

    if not candidates or _too_small(candidates[-1][1]):
        try:
            df_b = parse_layout_multi_band(img_proc)
            candidates.append(("multi_band", df_b))
        except Exception:
            pass

    # Fallback 2: column-aware pass using detected header x-positions
    if not candidates or _too_small(candidates[-1][1]):
        try:
            hdr_pairs = detect_period_columns_xy(img)
            df_c = parse_finance_from_image_colaware(img, hdr_pairs)
            candidates.append(("colaware", df_c))
        except Exception:
            pass

    # Fallback 3: line-based column-aware pass with y-clustering (usually richer rows)
    try:
        if not candidates or _too_small(candidates[-1][1]):
            hdr_pairs = detect_period_columns_xy(img)
            if hdr_pairs:
                lines = ocr_lines(img)
                labels = [lab for lab, _x in hdr_pairs]
                col_xy = {lab: x for lab, x in hdr_pairs}
                df_d, _dbg = parse_finance_lines(lines, col_labels=labels, col_positions=col_xy, max_vals=5, y_tol=10)
                candidates.append(("lines_ycluster", df_d))
    except Exception:
        pass

    # Fallback 4: micro-ROI numeric OCR per column center
    if not candidates or _too_small(candidates[-1][1]):
        try:
            df_e = _parse_with_strip_ocr(img)
            candidates.append(("roi_strip", df_e))
        except Exception:
            pass

    # Fallback 5: products-mode reconstruction by numeric rows
    if not candidates or _too_small(candidates[-1][1]):
        try:
            df_f = _parse_products_by_lines(img)
            candidates.append(("products_lines_fallback", df_f))
            if (df_f is None or df_f.empty) and callable(globals().get('_parse_products_by_centers')):
                df_g = _parse_products_by_centers(img)
                candidates.append(("products_centers_fallback", df_g))
        except Exception:
            pass

    # If a parser is forced, honor it
    if force:
        force_map = {
            'layout_v4': 'layout_v4',
            'multi_band': 'multi_band',
            'colaware': 'colaware',
            'lines': 'lines_ycluster',
            'lines_ycluster': 'lines_ycluster',
            'roi': 'roi_strip',
            'roi_strip': 'roi_strip',
            'products_lines': 'products_lines_fallback',
            'products_centers': 'products_centers_fallback',
        }
        key = force_map.get(force)
        if key:
            for name, cand in candidates:
                if name == key:
                    try:
                        DEBUG_DIR.mkdir(parents=True, exist_ok=True)
                        with open(DEBUG_DIR / f"p{page_num:02d}_selected_parser.txt", 'w', encoding='utf-8') as f:
                            f.write(f"forced: {name}\n")
                    except Exception:
                        pass
                    if isinstance(cand, pd.DataFrame) and not cand.empty:
                        df_forced = cand
                    else:
                        df_forced = cand
                    # finish pipeline on forced df (bypass scoring below)
                    if df_forced is None or (hasattr(df_forced,'empty') and df_forced.empty):
                        # continue to scoring if forced candidate was empty
                        pass
                    else:
                        df = df_forced
                        # write outputs and exit
                        try:
                            DEBUG_DIR.mkdir(parents=True, exist_ok=True)
                            df.to_csv(DEBUG_DIR / f"p{page_num:02d}_table.csv", index=False, encoding='utf-8-sig')
                        except Exception:
                            pass
                        try:
                            _rewrite_layout_overlay(img, page_num)
                        except Exception:
                            pass
                        return df

    # Choose the best candidate by heuristic score
    best_df = None
    best_name = None
    best_score = 1e9
    debug_scores = []
    cand_map = {}
    for name, cand in candidates:
        cand_map[name] = cand
        sc = _score_products_df(cand)
        debug_scores.append((name, sc, 0 if cand is None else len(cand)))
        if sc < best_score:
            best_score, best_df, best_name = sc, cand, name

    # Fallback: pick the first non-empty candidate if scoring didn't select one
    if best_df is None:
        for name, cand in candidates:
            if isinstance(cand, pd.DataFrame) and not cand.empty:
                best_df, best_name = cand, name
                break

    # If best is too small, prefer a richer candidate by row-count
    def _nrows(x):
        try:
            return 0 if x is None else (0 if not hasattr(x, 'shape') else int(x.shape[0]))
        except Exception:
            return 0
    if best_df is None or _nrows(best_df) < 8:
        # find candidate with max rows
        alt_name, alt_df, alt_rows = None, None, 0
        for n, _c in cand_map.items():
            r = _nrows(_c)
            if r > alt_rows:
                alt_name, alt_df, alt_rows = n, _c, r
        if alt_df is not None and alt_rows >= 8:
            best_name, best_df = alt_name, alt_df

    df = best_df
    # write which parser got selected and candidate scores
    try:
        DEBUG_DIR.mkdir(parents=True, exist_ok=True)
        with open(DEBUG_DIR / f"p{page_num:02d}_selected_parser.txt", 'w', encoding='utf-8') as f:
            f.write(f"selected: {best_name} score={best_score}\n")
            for n, s, rows in debug_scores:
                f.write(f"  - {n}: score={s} rows={rows}\n")
    except Exception:
        pass

    if df is None or df.empty:
        return None

    # Rename generic PeriodN columns to detected labels when available
    try:
        labels, _col_xy = _detect_headers_for_image(img_proc)
        if labels:
            df = _rename_value_columns(df, labels)
            # Keep at most 5 value columns in a preferred order
            df = _unify_columns(df)
    except Exception:
        pass

    # final clean and ordering
    if "Category" in df.columns:
        df["Category"] = df["Category"].map(_canon_label)
        value_cols = [c for c in df.columns if c != "Category"]
        for c in value_cols:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        # Try to compute products totals before dropping any all-empty rows
        try:
            df = _ensure_products_totals(df)
        except Exception:
            pass
        if value_cols:
            df.dropna(subset=value_cols, how='all', inplace=True)
        # drop header-band echo lines, then order like statement
        df = df[~df["Category"].apply(_is_header_label)]
        try:
            df = order_like_statement(df)
        except Exception:
            pass

    # sanitize tiny values that could have slipped through in month columns
    df = _sanitize_small_month_values(df)
    # if still too small, prefer richer candidate (products_lines) as last resort
    try:
        if df is not None and hasattr(df, 'shape') and df.shape[0] < 8 and cand_map.get('products_lines') is not None:
            df2 = cand_map['products_lines']
            if df2 is not None and hasattr(df2, 'shape') and df2.shape[0] >= 8:
                df = _sanitize_small_month_values(df2)
    except Exception:
        pass

    # Products-specific: if we have the 5 period columns, try to fill per-cell
    # gaps via micro-ROI OCR, then compute totals.
    try:
        hdr_pairs = detect_period_columns_xy(img)
        hdr_labels = [lab for lab, _ in hdr_pairs]
        val_cols = [c for c in df.columns if c != "Category"] if isinstance(df, pd.DataFrame) else []
        if isinstance(df, pd.DataFrame) and df.shape[1] >= 3 and any(k in val_cols for k in hdr_labels):
            # Anchor medical customers first so later fills can't spill
            df = _fill_medical_anchor_cells(img, df)
            df = _fill_missing_products_by_roi(img, df)
            df = _fill_missing_from_tokens_by_index(img, df)
            df = _ensure_products_totals(df)
            try:
                df = order_like_statement(df)
            except Exception:
                pass
    except Exception:
        pass

    # Final small-month sanitization after all fills to squash any reintroduced header/footnote fragments
    df = _sanitize_small_month_values(df)

    # Merge in missing month/forecast values from alternate parsers if available (layout_v4, colaware)
    try:
        alt_v4 = None
        alt_col = None
        try:
            alt_v4 = parse_by_layout_v4(img_proc, page_num=page_num)
        except Exception:
            alt_v4 = None
        try:
            hdr_pairs = detect_period_columns_xy(img)
            alt_col = parse_finance_from_image_colaware(img, hdr_pairs)
        except Exception:
            alt_col = None
        # prefer layout_v4, then colaware
        if isinstance(alt_v4, pd.DataFrame) and not alt_v4.empty:
            df = _merge_prefer_filled(df, alt_v4, only_months=True)
        if isinstance(alt_col, pd.DataFrame) and not alt_col.empty:
            df = _merge_prefer_filled(df, alt_col, only_months=True)
        df = _sanitize_small_month_values(df)
    except Exception:
        pass

    # Always write the final per-page table for debugging regardless of which parser path produced it
    try:
        DEBUG_DIR.mkdir(parents=True, exist_ok=True)
        df.to_csv(DEBUG_DIR / f"p{page_num:02d}_table.csv", index=False, encoding="utf-8-sig")
    except Exception:
        pass
    # Always rewrite the layout overlay PNG so timestamp updates
    try:
        _rewrite_layout_overlay(img, page_num)
    except Exception:
        pass
    # Debug dump for colaware assignments
    try:
        import csv
        (DEBUG_DIR / 'cells').mkdir(parents=True, exist_ok=True)
        with open(DEBUG_DIR / f"cells/p01_assignments_colaware.csv", 'w', newline='', encoding='utf-8') as f:
            w = csv.DictWriter(f, fieldnames=['category','col','text','x','y','reason'])
            w.writeheader()
            for a in assignments:
                w.writerow(a)
        with open(DEBUG_DIR / f"p01_assignments_colaware.csv", 'w', newline='', encoding='utf-8') as f2:
            w2 = csv.DictWriter(f2, fieldnames=['category','col','text','x','y','reason'])
            w2.writeheader()
            for a in assignments:
                w2.writerow(a)
        (DEBUG_DIR / f"p01_header_bottom.txt").write_text(str(header_bottom), encoding='utf-8')
    except Exception:
        pass
    return df


# -------------------- Table detection + cell OCR --------------------

from typing import List, Tuple

def _is_good_df(df: Optional[pd.DataFrame]) -> bool:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return False
    # needs at least 1 value column and at least 3 rows to be useful
    val_cols = [c for c in df.columns if c != "Category"]
    return len(val_cols) >= 1 and len(df) >= 2

def _finalize_page_df(df: pd.DataFrame) -> pd.DataFrame:
    # light finalization for a single page
    df = df.copy()
    if "Category" in df.columns:
        df["Category"] = df["Category"].map(lambda s: _canon_label(s or ""))
    for c in [x for x in df.columns if x != "Category"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(how='all', subset=[c for c in df.columns if c != "Category"])
    return df









def _ensure_products_totals(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty or 'Category' not in df.columns:
        return df
    out = df.copy()
    orig = df.copy()
    val_cols = [c for c in out.columns if c != 'Category']

    def _idx(name: str):
        k = _canon_label(name)
        m = out['Category'].map(_canon_label).eq(k)
        return int(m.idxmax()) if m.any() else None

    # Prepare component groups
    med_parts = ["customer #1","customer #2","customer #3","customer #4","other medical customers"]
    ind_parts = ["matthew","mark","luke","john","peter"]
    have_med = out['Category'].map(_canon_label).isin([_canon_label(x) for x in med_parts]).any()
    have_ind = out['Category'].map(_canon_label).isin([_canon_label(x) for x in ind_parts]).any()
    def _ensure_row(label: str):
        if _idx(label) is None:
            new = {c: pd.NA for c in out.columns}
            new['Category'] = _canon_label(label)
            out.loc[len(out)] = new
    # Ensure rows exist but we will only fill values when missing
    if have_med:
        _ensure_row("total medical products")
    if have_ind:
        _ensure_row("total industrial products")
    if have_med or have_ind:
        _ensure_row("total revenue")
        _ensure_row("total aps, inc. revenue")

    def _set_if_missing(name: str, col: str, val):
        i = _idx(name)
        if i is None or col not in out.columns:
            return
        if pd.isna(out.at[i, col]) or out.at[i, col] is None or str(out.at[i, col]).strip()=='' :
            out.at[i, col] = val
    def _set_if_differs(name: str, col: str, val, tol_ratio: float = 0.005):
        i = _idx(name)
        if i is None or col not in out.columns:
            return
        cur = pd.to_numeric(out.at[i, col], errors='coerce')
        if pd.isna(cur):
            out.at[i, col] = val
            return
        try:
            if not pd.isna(val):
                if abs(float(cur) - float(val)) > max(1.0, tol_ratio * max(abs(float(val)), 1.0)):
                    out.at[i, col] = val
        except Exception:
            out.at[i, col] = val

    med_comps = med_parts
    ind_comps = ind_parts

    cats = out['Category'].map(_canon_label)
    med_mask = cats.isin([_canon_label(x) for x in med_comps])
    ind_mask = cats.isin([_canon_label(x) for x in ind_comps])

    for col in val_cols:
        try:
            med_sum = pd.to_numeric(out.loc[med_mask, col], errors='coerce').sum(skipna=True)
            if med_mask.any() and not pd.isna(med_sum) and med_sum != 0:
                # Always enforce computed totals (override OCR mistakes)
                _set_if_differs("total medical products", col, float(med_sum))
        except Exception:
            pass
        try:
            ind_sum = pd.to_numeric(out.loc[ind_mask, col], errors='coerce').sum(skipna=True)
            if ind_mask.any() and not pd.isna(ind_sum) and ind_sum != 0:
                _set_if_differs("total industrial products", col, float(ind_sum))
        except Exception:
            pass
        # Total revenue and APS revenue from med+ind totals, but only if missing
        try:
            i_med = _idx("total medical products")
            i_ind = _idx("total industrial products")
            if i_med is not None and i_ind is not None:
                med_total = pd.to_numeric(out.at[i_med, col], errors='coerce')
                ind_total = pd.to_numeric(out.at[i_ind, col], errors='coerce')
                if pd.notna(med_total) and pd.notna(ind_total):
                    total_rev = float(med_total) + float(ind_total)
                    _set_if_differs("total revenue", col, total_rev)
                    _set_if_differs("total aps, inc. revenue", col, total_rev)
        except Exception:
            pass

    # Derive missing single component by difference if total is known (prefer printed totals)
    for col in val_cols:
        # Industrial components
        try:
            vals = {name: pd.to_numeric(out.at[_idx(name), col], errors='coerce') if _idx(name) is not None else pd.NA
                    for name in ind_comps}
            known = [float(v) for v in vals.values() if pd.notna(v)]
            missing_names = [n for n, v in vals.items() if pd.isna(v)]
            ti = _idx("total industrial products")
            ti_orig = ti
            if ti is not None and len(missing_names) == 1:
                # Prefer printed total from original table if available
                total_v = pd.to_numeric(orig.at[ti_orig, col], errors='coerce') if ti_orig is not None else pd.NA
                if pd.isna(total_v):
                    total_v = pd.to_numeric(out.at[ti, col], errors='coerce')
                if pd.notna(total_v):
                    derived = float(total_v) - sum(known)
                    out.at[_idx(missing_names[0]), col] = derived
        except Exception:
            pass
        # Medical components
        try:
            vals = {name: pd.to_numeric(out.at[_idx(name), col], errors='coerce') if _idx(name) is not None else pd.NA
                    for name in med_comps}
            known = [float(v) for v in vals.values() if pd.notna(v)]
            missing_names = [n for n, v in vals.items() if pd.isna(v)]
            tm = _idx("total medical products")
            tm_orig = tm
            if tm is not None and len(missing_names) == 1:
                total_v = pd.to_numeric(orig.at[tm_orig, col], errors='coerce') if tm_orig is not None else pd.NA
                if pd.isna(total_v):
                    total_v = pd.to_numeric(out.at[tm, col], errors='coerce')
                if pd.notna(total_v):
                    derived = float(total_v) - sum(known)
                    out.at[_idx(missing_names[0]), col] = derived
        except Exception:
            pass

    # Re-enforce totals after derivations
    for col in val_cols:
        try:
            med_sum = pd.to_numeric(out.loc[med_mask, col], errors='coerce').sum(skipna=True)
            if med_mask.any() and not pd.isna(med_sum) and med_sum != 0:
                _set_if_differs("total medical products", col, float(med_sum))
        except Exception:
            pass
        try:
            ind_sum = pd.to_numeric(out.loc[ind_mask, col], errors='coerce').sum(skipna=True)
            if ind_mask.any() and not pd.isna(ind_sum) and ind_sum != 0:
                _set_if_differs("total industrial products", col, float(ind_sum))
        except Exception:
            pass
        try:
            i_med = _idx("total medical products")
            i_ind = _idx("total industrial products")
            if i_med is not None and i_ind is not None:
                med_total = pd.to_numeric(out.at[i_med, col], errors='coerce')
                ind_total = pd.to_numeric(out.at[i_ind, col], errors='coerce')
                if pd.notna(med_total) and pd.notna(ind_total):
                    total_rev = float(med_total) + float(ind_total)
                    _set_if_differs("total revenue", col, total_rev)
                    _set_if_differs("total aps, inc. revenue", col, total_rev)
        except Exception:
            pass

    # Derive missing single component from totals (generic, no hard-coding of values)
    groups = [
        ("total medical products", ["customer #1","customer #2","customer #3","customer #4","other medical customers"]),
        ("total industrial products", ["matthew","mark","luke","john","peter"]),
    ]
    for total_name, parts in groups:
        t_idx = _idx(total_name)
        if t_idx is None:
            continue
        for col in val_cols:
            total_v = to_number(out.at[t_idx, col])
            if not isinstance(total_v,(int,float)):
                continue
            vals = []
            miss = None
            for p in parts:
                i = _idx(p)
                if i is None:
                    continue
                v = to_number(out.at[i, col])
                if isinstance(v,(int,float)):
                    vals.append(v)
                else:
                    if miss is None:
                        miss = i
                    else:
                        miss = 'multi'  # more than one missing
                        break
            if miss is not None and miss != 'multi':
                remain = sum(vals)
                out.at[miss, col] = total_v - remain

    return out


def _fill_missing_products_by_roi(img: Image.Image, df: pd.DataFrame) -> pd.DataFrame:
    try:
        pairs = detect_period_columns_xy(img)
    except Exception:
        return df
    if not pairs:
        return df
    labels  = [lab for lab, _ in pairs if lab in df.columns]
    centers = [float(x) for lab, x in pairs if lab in df.columns]
    if not labels:
        return df

    # rebuild row y anchors from numbers
    lines = ocr_lines(img)
    nums = []
    for ln in lines:
        for t in (ln.get('tokens') or []):
            s = str(t.get('t','')).strip()
            v = to_number(s)
            if isinstance(v,(int,float)):
                nums.append({'x': int(t.get('x',0)), 'y': int(t.get('y',0)), 'val': v})
    if not nums:
        return df
    ys = _group_rows_by_y([n['y'] for n in nums], tol=14)

    arr = np.array(img.convert('L'))
    H, W = arr.shape[:2]
    out = df.copy()
    # Protect anchored medical rows for May-25 / June Forecast
    protect_cols = [c for c in ("May-25", "June Forecast") if c in labels]
    med_order = ["customer #1", "customer #2", "customer #3", "customer #4", "other medical customers"]
    med_row_indices = []
    for name in med_order:
        idxs = out.index[out["Category"].map(_canon_label) == name].tolist()
        if idxs:
            med_row_indices.append(int(idxs[0]))
    # Derive med anchors from left-of-first-column labels
    med_anchor_by_label: dict[str,int] = {}
    try:
        first_x = min(centers) if centers else None
        if first_x is not None:
            left_rows: list[tuple[int,str]] = []
            for ln in lines:
                toks = ln.get('tokens') or []
                words = [str(t.get('t','')).strip() for t in toks if int(t.get('x',0)) < int(first_x) - 18 and re.search(r"[A-Za-z#]", str(t.get('t','')))]
                if not words:
                    continue
                label = _canon_label(' '.join(w for w in words if w).strip())
                if not label or _is_header_label(label):
                    continue
                y = int(ln.get('y', 0))
                if label in med_order and label not in med_anchor_by_label:
                    med_anchor_by_label[label] = y
    except Exception:
        med_anchor_by_label = {}
    # Protect medical customer rows for May-25/June Forecast (handled by anchored fill)
    protect_cols = [c for c in ("May-25", "June Forecast") if c in labels]
    med_order = ["customer #1", "customer #2", "customer #3", "customer #4", "other medical customers"]
    med_row_indices = []
    for name in med_order:
        idxs = out.index[out["Category"].map(_canon_label) == name].tolist()
        if idxs:
            med_row_indices.append(int(idxs[0]))
    rows = min(len(out), len(ys))
    for i in range(rows):
        # Use med anchors for med rows if available, else fallback to ys[i]
        cat = _canon_label(str(out.iloc[i]["Category"])) if i < len(out) and "Category" in out.columns else ""
        y = int(med_anchor_by_label.get(cat, ys[i]))
        for col_idx, (lab, cx) in enumerate(zip(labels, centers)):
            if pd.notna(out.at[i, lab]):
                continue
            # Skip protected cells only if already filled by anchored pass; otherwise allow generic fill
            if (i in med_row_indices) and (lab in protect_cols) and pd.notna(out.at[i, lab]):
                continue
            x = int(cx)
            # progressively expand within safe gate bounds
            base_extra = 26 if col_idx in (0,1,2) else 12
            saved_any = False
            got = False
            for t_i, extra in enumerate((base_extra, base_extra+10, base_extra+18), start=1):
                x1 = max(0, x - (32 + extra)); x2 = min(W, x + (32 + extra))
                y1 = max(0, y - 14); y2 = min(H, y + 20)
                if x2 <= x1 or y2 <= y1:
                    continue
                roi = arr[y1:y2, x1:x2]
                v = None
                if (i in med_row_indices) and (lab in protect_cols):
                    # geometry-aware for medical restricted cells only
                    try:
                        local_target = float(x - x1)
                        gv = _best_num_from_roi(roi, local_target, strategy='right')
                    except Exception:
                        gv = None
                    if isinstance(gv, (int, float)):
                        # suppress tiny month numerics (e.g., -25, 1)
                        if re.search(r"(?i)\b(jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)|forecast\b", str(lab)) and abs(gv) < 100:
                            v = None
                        else:
                            v = gv
                if v is None:
                    try:
                        txt = pytesseract.image_to_string(roi, config=NUM_TESS_CFG).strip()
                        v = to_number(txt)
                        if v is None:
                            txt2 = pytesseract.image_to_string(roi, config='--oem 1 --psm 7 -c preserve_interword_spaces=1').strip()
                            v = to_number(txt2)
                        if v is None:
                            txt3 = pytesseract.image_to_string(roi, config='--oem 1 --psm 8').strip()
                            v = to_number(txt3)
                    except Exception:
                        v = None
                # always save attempted ROI for visibility
                try:
                    (DEBUG_DIR / 'cells').mkdir(parents=True, exist_ok=True)
                    Image.fromarray(roi).save(DEBUG_DIR / f"cells/cell_r{i:02d}_{lab.replace(' ','_')}_try{t_i}.png")
                    saved_any = True
                except Exception:
                    pass
                if isinstance(v,(int,float)):
                    if re.search(r"(?i)\b(jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)|forecast\b", str(lab)) and abs(v) < 100:
                        # skip tiny month values
                        pass
                    else:
                        out.at[i, lab] = v
                    got = True
                    break
    return out


def _fill_missing_from_tokens_by_index(img: Image.Image, df: pd.DataFrame) -> pd.DataFrame:
    """
    Use ocr_lines() numeric tokens snapped to header centers, grouped by y-index,
    to fill any remaining missing cells in a Products-style table.
    Assumes df rows are in PRODUCTS_EXPECTED order (or a prefix of it).
    """
    try:
        pairs = detect_period_columns_xy(img)
    except Exception:
        return df
    if not pairs:
        return df
    labels  = [lab for lab, _ in pairs if lab in df.columns]
    centers = [float(x) for lab, x in pairs if lab in df.columns]
    if not labels:
        return df

    lines = ocr_lines(img)
    if not lines:
        return df

    # Gather numeric tokens with (x,y,val) from lines
    toks = []
    H = img.size[1]
    for ln in lines:
        for t in (ln.get('tokens') or []):
            s = str(t.get('t','')).strip()
            v = to_number(s)
            if isinstance(v, (int, float)):
                x = int(t.get('x', 0)); y = int(t.get('y', 0))
                w = int(t.get('w', 0)) if isinstance(t.get('w', 0), (int, float)) else 0
                # Use right-edge for right-aligned numbers if enabled
                if ALIGN_NUMS_RIGHT:
                    x = x + max(0, int(round(w/2)))
                # suppress footer tiny numerics (e.g., page numbers like "1.2" -> "12")
                if (y + max(0, int(t.get('h', 0)))) >= int(H * 0.96):
                    if abs(v) < 50:
                        continue
                toks.append({'x': x, 'y': y, 'val': v})
    if not toks:
        return df

    # Cluster y into canonical rows and filter out header-band clusters
    ys = _group_rows_by_y([t['y'] for t in toks], tol=12)
    # detect header y's from lines text
    header_ys = []
    for ln in lines:
        txt = str(ln.get('text','')).lower()
        if any(k in txt for k in ['forecast','ytd','jun','may']):
            if ln.get('y', None) is not None:
                header_ys.append(int(ln.get('y')))
    ys = [y for y in ys if not any(abs(y - hy) <= 10 for hy in header_ys)]
    if not ys:
        return df

    # If right alignment is enabled, attempt to derive per-column right anchors
    anchors = list(centers)
    if ALIGN_NUMS_RIGHT:
        import statistics as _stats
        ra = []
        for i, cx in enumerate(centers):
            near = [t['x'] for t in toks if abs(t['x'] - cx) <= 60]
            if near:
                try:
                    ra.append(float(_stats.median(near)))
                except Exception:
                    ra.append(cx)
            else:
                ra.append(cx)
        anchors = ra

    # Column boundaries (halfway between anchors). Left/right extend to extremes.
    bounds = []
    for i in range(len(anchors)+1):
        if i == 0:
            b = anchors[0] - max(20, (anchors[1]-anchors[0])/2.0)
        elif i == len(anchors):
            b = anchors[-1] + max(20, (anchors[-1]-anchors[-2])/2.0)
        else:
            b = (anchors[i-1] + anchors[i]) / 2.0
        bounds.append(float(b))

    # Row boundaries
    ybounds = []
    for i in range(len(ys)+1):
        if i == 0:
            yb = ys[0] - 10.0
        elif i == len(ys):
            yb = ys[-1] + 12.0
        else:
            yb = (ys[i-1] + ys[i]) / 2.0
        ybounds.append(float(yb))

    # Build grid mapping (col i, row j) -> best value inside the rectangular gate
    grid: dict[tuple[int,int], float] = {}
    # allow extra horizontal slack for May-25 and June Forecast columns
    col_extra = {1: 20.0, 2: 20.0}
    for t in toks:
        # column index by boundaries
        ci = None
        for i in range(len(bounds)-1):
            lb = bounds[i] - col_extra.get(i, 0.0)
            ub = bounds[i+1] + col_extra.get(i, 0.0)
            if lb <= t['x'] < ub:
                ci = i; break
        if ci is None or ci >= len(labels):
            continue
        # row index by y-boundaries
        rj = None
        for j in range(len(ybounds)-1):
            if ybounds[j] <= t['y'] < ybounds[j+1]:
                rj = j; break
        if rj is None:
            continue
        key = (ci, rj)
        # keep the value closest to the row center
        cur = grid.get(key)
        if cur is None or abs(t['y'] - ys[rj]) < abs(cur[1] - ys[rj]):
            grid[key] = (t['val'], t['y'])

    out = df.copy()
    # Map df row index -> y index (by position, with bounds)
    max_rows = min(len(PRODUCTS_EXPECTED), len(ys))
    # prepare med row indices and protect cols from earlier context if not in scope
    try:
        med_order = ["customer #1","customer #2","customer #3","customer #4","other medical customers"]
        med_row_indices = []
        for name in med_order:
            idxs = out.index[out["Category"].map(_canon_label) == name].tolist()
            if idxs:
                med_row_indices.append(int(idxs[0]))
    except Exception:
        med_row_indices = []
    protect_cols = [c for c in ("May-25", "June Forecast") if c in labels]
    for r in range(min(len(out), max_rows)):
        for i, lab in enumerate(labels):
            # Skip protected med rows/cols to avoid spill
            if (r in med_row_indices) and (lab in protect_cols):
                continue
            if pd.isna(out.at[r, lab]) or out.at[r, lab] is None or str(out.at[r, lab]).strip()=='' :
                tup = grid.get((i, r))
                if tup is not None:
                    vfill = tup[0]
                    # suppress tiny month values for month/forecast columns
                    if re.search(r"(?i)\b(jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)|forecast\b", str(lab)) and isinstance(vfill, (int,float)) and abs(vfill) < 100:
                        pass
                    else:
                        out.at[r, lab] = vfill
                else:
                    # fallback: use the nearest token in this column to the row center (looser y tolerance)
                    y0 = ys[r]
                    col_candidates = [(key, val) for key, val in grid.items() if key[0] == i]
                    if col_candidates:
                        best_key, best = min(col_candidates, key=lambda kv: abs(kv[1][1] - y0))
                        if abs(best[1] - y0) <= 36:
                            vfill = best[0]
                            if re.search(r"(?i)\b(jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)|forecast\b", str(lab)) and isinstance(vfill, (int,float)) and abs(vfill) < 100:
                                pass
                            else:
                                out.at[r, lab] = vfill
    # --- DEBUG: tokens + overlay ---
    try:
        DEBUG_DIR.mkdir(parents=True, exist_ok=True)
        import csv, json
        from PIL import Image, ImageDraw
        with open(DEBUG_DIR / 'products_tokens.csv', 'w', newline='', encoding='utf-8') as f:
            w = csv.writer(f); w.writerow(['x','y','val'])
            for t in toks: w.writerow([t['x'], t['y'], t['val']])
        (DEBUG_DIR / 'products_meta.json').write_text(json.dumps({
            'labels': labels, 'centers': centers, 'ys': ys
        }, indent=2))
        # overlay
        vis = img.convert('RGB').copy(); dr = ImageDraw.Draw(vis)
        H = vis.size[1]
        # Draw standard centers and, if right-align in use, the right anchors
        for cx in centers:
            dr.line([(cx,0),(cx,H)], fill=(0,180,0), width=1)
        if ALIGN_NUMS_RIGHT:
            for rx in anchors:
                dr.line([(rx,0),(rx,H)], fill=(0,255,255), width=1)
        for y in ys:
            dr.line([(0,y),(vis.size[0],y)], fill=(220,60,60), width=1)
        for t in toks:
            dr.rectangle([t['x']-2, t['y']-2, t['x']+2, t['y']+2], outline=(255,200,0))
        vis.save(DEBUG_DIR / 'p01_products_overlay.png')
    except Exception:
        pass
    return out


def _is_dash_only(s: str) -> bool:
    s = (s or "").strip()
    return s in {"-", "–", "—", "–", "·", "•", "_"}


def _fill_medical_anchor_cells(img: Image.Image, df: pd.DataFrame) -> pd.DataFrame:
    """
    Anchor May-25 and June Forecast fills for the first 5 medical rows
    using the row y-bands derived from numeric tokens, so values cannot
    spill to the next row. Also treat dash-like cells as 0.
    """
    if df is None or df.empty or 'Category' not in df.columns:
        return df
    try:
        pairs = detect_period_columns_xy(img)
    except Exception:
        return df
    if not pairs:
        return df
    labels  = [lab for lab, _ in pairs]
    centers = [int(x) for _, x in pairs]

    # we specifically need these two columns
    target_cols = ['May-25', 'June Forecast']
    have_cols = [c for c in target_cols if c in df.columns]
    if not have_cols:
        return df

    # Build per-row anchors from the left-of-first-column label lines, so values
    # are aligned by the row label itself (avoids shifting across rows).
    lines = ocr_lines(img)
    first_x = min(centers) if centers else None
    if first_x is None:
        return df

    # Collect (y,label) from lines that have left-of-first-column text
    left_rows: list[tuple[int, str]] = []
    for ln in lines:
        toks = ln.get('tokens') or []
        left_words = [str(t.get('t','')).strip() for t in toks if int(t.get('x',0)) < int(first_x) - 18 and re.search(r"[A-Za-z#]", str(t.get('t','')))]
        if not left_words:
            continue
        label = ' '.join(w for w in left_words if w).strip()
        if not label or _is_header_label(label):
            continue
        y = int(ln.get('y', 0))
        left_rows.append((y, _canon_label(label)))

    # Keep only the medical rows in visual top->bottom order
    med_order = [
        "customer #1", "customer #2", "customer #3", "customer #4", "other medical customers",
    ]
    anchors_by_label: dict[str,int] = {}
    for y, lab in sorted(left_rows, key=lambda z: z[0]):
        if lab in med_order and lab not in anchors_by_label:
            anchors_by_label[lab] = y
        if len(anchors_by_label) == 5:
            break
    # Fallback to previous numeric-based anchors if we didn't get at least two
    if len(anchors_by_label) < 2:
        jun_cx = centers[0] if centers else None
        if jun_cx is None:
            return df
        header_y = None
        for ln in lines:
            txt = str(ln.get('text','')).lower()
            if all(k in txt for k in ['jun','may','forecast']) or 'ytd' in txt:
                header_y = int(ln.get('y', 0))
                break
        col_tokens_y = []
        for ln in lines:
            for t in (ln.get('tokens') or []):
                s = str(t.get('t','')).strip()
                v = to_number(s)
                if not isinstance(v,(int,float)):
                    continue
                x = int(t.get('x',0)); y = int(t.get('y',0))
                if abs(x - jun_cx) <= 42:
                    if header_y is not None and y <= (header_y + 6):
                        continue
                    col_tokens_y.append(y)
        if not col_tokens_y:
            return df
        ys = _group_rows_by_y(col_tokens_y, tol=10)
        if not ys:
            return df
        anchors = [int(y) for y in ys[:5]]
        # Map in med_order
        anchors_by_label = {lab: anchors[i] for i, lab in enumerate(med_order[:len(anchors)])}

    # Build global value token y-centers across all columns to refine anchors
    try:
        vt_all = []
        for ln in lines:
            for t in (ln.get('tokens') or []):
                s = str(t.get('t','')).strip()
                v = to_number(s)
                if not isinstance(v,(int,float)):
                    continue
                x = int(t.get('x',0)); y = int(t.get('y',0))
                if any(abs(x - int(cx)) <= 42 for cx in centers):
                    vt_all.append(int(y))
        ys_all = _group_rows_by_y(vt_all, tol=12) if vt_all else []
        if ys_all:
            refined = {}
            for lab, y in anchors_by_label.items():
                yn = min(ys_all, key=lambda yy: abs(yy - y))
                refined[lab] = int(yn)
            anchors_by_label = refined
    except Exception:
        pass
    # Anchors in med_order for consistent row-index mapping
    anchors = [anchors_by_label.get(lab, None) for lab in med_order]
    anchors = [a for a in anchors if a is not None]
    # Debug: write anchors to inspect
    try:
        (DEBUG_DIR).mkdir(parents=True, exist_ok=True)
        Path(DEBUG_DIR, 'med_anchors.json').write_text(json.dumps({
            'anchors_by_label': anchors_by_label,
            'ordered_anchors': anchors
        }, indent=2))
    except Exception:
        pass

    # build row y-bounds from anchors to isolate rows
    ybounds = []
    for i in range(len(anchors)+1):
        if i == 0:
            ybounds.append(anchors[0] - 9)
        elif i == len(anchors):
            ybounds.append(anchors[-1] + 9)
        else:
            ybounds.append(int((anchors[i-1] + anchors[i]) / 2))

    # map column name -> center x
    cx_by_label = {lab: centers[i] for i, lab in enumerate(labels) if i < len(centers)}
    # compute a safe half-window per column based on neighbor gaps to avoid spill
    ordered = sorted([(lab, cx_by_label.get(lab, 0)) for lab in have_cols], key=lambda z: z[1])
    safe_half: dict[str, int] = {}
    for idx, (lab, cx) in enumerate(ordered):
        left_gap  = cx - ordered[idx-1][1] if idx > 0 else 64
        right_gap = ordered[idx+1][1] - cx if idx+1 < len(ordered) else 64
        half = int(max(24, min(left_gap, right_gap) / 2)) - 2
        if half < 16:
            half = 16
        safe_half[lab] = half
    arr = np.array(img.convert('L'))
    H, W = arr.shape[:2]
    out = df.copy()

    # Map anchors to actual medical customer rows by canonical label
    # Build list of (df_row_index, anchor_index) in visual order
    med_row_indices: list[int] = []
    for name in med_order:
        idxs = out.index[out["Category"].map(_canon_label) == name].tolist()
        if idxs:
            med_row_indices.append(int(idxs[0]))
    # Limit to available anchors
    n_pairs = min(len(med_row_indices), len(anchors))

    # Pre-index tokens per target column for backstop fill
    tok_by_col: dict[str, list[tuple[int,float]]] = {c: [] for c in have_cols}
    for ln in lines:
        for t in (ln.get('tokens') or []):
            s = str(t.get('t','')).strip()
            v = to_number(s)
            if not isinstance(v,(int,float)):
                continue
            x = int(t.get('x',0)); y = int(t.get('y',0))
            for col in have_cols:
                cx = int(cx_by_label.get(col, 0))
                if cx and abs(x - cx) <= 42:
                    # keep tokens that look like real numbers (>= 3 digits)
                    if len(__import__('re').sub(r'\D','', s)) >= 3:
                        tok_by_col[col].append((y, v))

    # sort tokens by y for deterministic consumption and track usage per column
    for k in list(tok_by_col.keys()):
        tok_by_col[k] = sorted(tok_by_col[k], key=lambda p: p[0])

    for j in range(n_pairs):
        df_row = med_row_indices[j]
        y0 = anchors[j]
        for col in have_cols:
            cx = int(cx_by_label.get(col, 0))
            if cx <= 0:
                continue
            # tight vertical window around anchor; modest horizontal slack
            half = int(safe_half.get(col, 32))
            x1 = max(0, cx - half); x2 = min(W, cx + half)
            # use row-specific y-bounds to avoid cross-row spill
            y1 = max(0, ybounds[j]); y2 = min(H, ybounds[j+1])
            if y2 <= y1:
                y1 = max(0, y0 - 10); y2 = min(H, y0 + 12)
            roi = arr[y1:y2, x1:x2]
            try:
                txt = pytesseract.image_to_string(roi, config=NUM_TESS_CFG).strip()
            except Exception:
                txt = ''
            v = to_number(txt)
            dcount = len(__import__('re').sub(r'\D','', txt))
            # If OCR is weak, try a secondary mode
            if v is None or dcount < 3:
                try:
                    txt2 = pytesseract.image_to_string(roi, config='--oem 1 --psm 7 -c preserve_interword_spaces=1').strip()
                except Exception:
                    txt2 = ''
                v2 = to_number(txt2)
                dcount2 = len(__import__('re').sub(r'\D','', txt2))
                if v2 is not None and dcount2 >= 3:
                    v, dcount = v2, dcount2

            # Choose value by precedence:
            #   token in this band (consume) ->
            #   for May-25 / June Forecast: ROI first (multi-try) -> nearest token (<=20px, consume) ->
            #   other cols: nearest token (<=45px, consume) -> ROI ->
            #   dash/blank -> clear
            col_list = tok_by_col.get(col, [])
            band_idx = None
            for idx, (yy, tv) in enumerate(col_list):
                if y1 <= yy < y2:
                    if band_idx is None or abs(col_list[idx][0] - y0) < abs(col_list[band_idx][0] - y0):
                        band_idx = idx
            if band_idx is not None:
                out.at[df_row, col] = col_list[band_idx][1]
                del col_list[band_idx]
            else:
                # For restricted columns, try ROI first with widened windows
                restricted = (col in ("May-25", "June Forecast"))
                if restricted:
                    got_roi = False
                    for extra in (0, 8, 16):
                        rx1 = max(0, x1 - extra); rx2 = min(W, x2 + extra)
                        rroi = arr[y1:y2, rx1:rx2]
                        rroi_alt = arr[max(0, y0-16):min(H, y0+18), rx1:rx2]
                        def _ocr_roi(a):
                            try:
                                return pytesseract.image_to_string(a, config=NUM_TESS_CFG).strip()
                            except Exception:
                                return ''
                        rtxt = _ocr_roi(rroi)
                        rv = to_number(rtxt)
                        rdc = len(__import__('re').sub(r'\D','', rtxt))
                        if rv is None or rdc < 3:
                            rtxt2 = ''
                            try:
                                rtxt2 = pytesseract.image_to_string(rroi, config='--oem 1 --psm 7 -c preserve_interword_spaces=1').strip()
                            except Exception:
                                pass
                            rv2 = to_number(rtxt2)
                            rdc2 = len(__import__('re').sub(r'\D','', rtxt2))
                            if rv2 is not None and rdc2 >= 3:
                                rv, rdc = rv2, rdc2
                        # try alternate vertical crop around anchor if still nothing
                        if rv is None or rdc < 3:
                            rtxtA = _ocr_roi(rroi_alt)
                            rvA = to_number(rtxtA)
                            rdcA = len(__import__('re').sub(r'\D','', rtxtA))
                            if rvA is not None and rdcA >= 3:
                                rv, rdc = rvA, rdcA
                        # Try OTSU binarization if still nothing
                        if rv is None:
                            try:
                                import cv2 as _cv
                                _, rbin = _cv.threshold(rroi, 0, 255, _cv.THRESH_BINARY + _cv.THRESH_OTSU)
                                rtxt3 = pytesseract.image_to_string(rbin, config=NUM_TESS_CFG).strip()
                            except Exception:
                                rtxt3 = ''
                            rv3 = to_number(rtxt3)
                            rdc3 = len(__import__('re').sub(r'\D','', rtxt3))
                            if rv3 is not None and rdc3 >= 3:
                                rv, rdc = rv3, rdc3
                        # Try geometry-aware pick inside ROI first
                        local_target = float(cx - rx1)
                        best_geo = _best_num_from_roi(rroi, local_target, strategy='right')
                        if isinstance(best_geo, (int, float)):
                            rv, rdc = best_geo, 3
                        if rv is not None and rdc >= 3:
                            out.at[df_row, col] = rv
                            got_roi = True
                            # Debug dump ROI
                            try:
                                (DEBUG_DIR / 'cells').mkdir(parents=True, exist_ok=True)
                                Image.fromarray(rroi).save(DEBUG_DIR / f"cells/anchored_r{j+1}_{col.replace(' ','_')}_extra{extra}.png")
                                Path(DEBUG_DIR, 'anchored_log.csv').write_text('', encoding='utf-8') if not (DEBUG_DIR / 'anchored_log.csv').exists() else None
                                with open(DEBUG_DIR / 'anchored_log.csv', 'a', encoding='utf-8') as _f:
                                    _f.write(f"row={j+1},col={col},extra={extra},x1={rx1},x2={rx2},y1={y1},y2={y2},txt={rtxt!r},{rtxt2!r}\n")
                            except Exception:
                                pass
                            break
                    if got_roi:
                        continue
                    # then nearest-by-anchor with tighter tolerance (<=20px)
                    all_toks = tok_by_col.get(col, [])
                    if all_toks:
                        best_i, (yy, tv) = min(enumerate(all_toks), key=lambda kv: abs(kv[1][0] - y0))
                        if abs(yy - y0) <= 20:
                            out.at[df_row, col] = tv
                            del all_toks[best_i]
                            continue
                else:
                    # nearest-by-anchor fallback for non-restricted columns
                    all_toks = tok_by_col.get(col, [])
                    if all_toks:
                        best_i, (yy, tv) = min(enumerate(all_toks), key=lambda kv: abs(kv[1][0] - y0))
                        if abs(yy - y0) <= 45:
                            out.at[df_row, col] = tv
                            del all_toks[best_i]
                            continue
                    # ROI for non-restricted
                    if v is not None and dcount >= 3:
                        out.at[df_row, col] = v
                if _is_dash_only(txt) or (txt.strip()=='' and (not tok_by_col.get(col))):
                    out.at[df_row, col] = 0.0
                else:
                    # No strong ROI text and no tokens in this row band -> clear any misassigned value
                    out.at[df_row, col] = None
        # Wide pair-capture for May-25 + June Forecast: if both exist in-band, assign by x-order
        try:
            if all(c in cx_by_label for c in ('May-25','June Forecast')) and j < len(ybounds)-1:
                mx = int(cx_by_label['May-25']); jx = int(cx_by_label['June Forecast'])
                wx1 = max(0, min(mx, jx) - 48); wx2 = min(W, max(mx, jx) + 48)
                wy1 = max(0, ybounds[j]); wy2 = min(H, ybounds[j+1])
                if wy2 <= wy1:
                    wy1 = max(0, y0 - 12); wy2 = min(H, y0 + 14)
                wroi = arr[wy1:wy2, wx1:wx2]
                df_tokens = pytesseract.image_to_data(wroi, output_type=Output.DATAFRAME,
                    config="--psm 6 --oem 1 -c preserve_interword_spaces=1")
                df_tokens = df_tokens.dropna(subset=['text'])
                cand = []
                for _, r in df_tokens.iterrows():
                    s = str(r.get('text','')).strip()
                    v = to_number(s)
                    if not isinstance(v,(int,float)):
                        continue
                    cx_local = float(r.get('left',0)) + float(r.get('width',0))/2.0
                    cand.append((cx_local, v))
                cand.sort(key=lambda z: z[0])
                if len(cand) >= 2:
                    # pick the two with largest separation so we don't take '4,213 500' as pair
                    # heuristic: farthest-apart pair
                    best_pair = None; best_gap = -1
                    for a in range(len(cand)):
                        for b in range(a+1, len(cand)):
                            gap = cand[b][0] - cand[a][0]
                            if gap > best_gap:
                                best_gap = gap; best_pair = (cand[a], cand[b])
                    (xL, vL), (xR, vR) = best_pair
                    out.at[df_row, 'May-25'] = vL
                    out.at[df_row, 'June Forecast'] = vR
        except Exception:
            pass
    return out


# Note: we intentionally avoid hard-coded data overrides.


def _rename_value_columns(df: pd.DataFrame, col_labels: list[str]) -> pd.DataFrame:
    """
    If df has value columns like Period1, Period2... rename them to detected labels.
    Keeps 'Category' first; renames in positional order for the rest.
    Extra/missing detected labels are handled gracefully.
    """
    if df is None or df.empty or not col_labels:
        return df

    # keep order: Category, then the current non-meta numeric columns
    val_cols = [c for c in df.columns if c != "Category" and not str(c).startswith("__")]
    if not val_cols:
        return df

    # build mapping PeriodN -> label
    new_names = {}
    for i, c in enumerate(val_cols):
        if i < len(col_labels):
            new_names[c] = col_labels[i]

    if new_names:
        df = df.rename(columns=new_names)

    # force numeric after rename (safety)
    for c in [x for x in df.columns if x != "Category" and not str(x).startswith("__")]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    return df


def _attach_categories_from_lines(
    df: pd.DataFrame,
    lines,                      # whatever your ocr_lines() returns
    col_xy: dict[str, float] | None,
    y_tolerance: float = 14.0,
) -> pd.DataFrame:
    """
    For each numeric row (which has a __y from parse_finance_lines), attach the closest
    left-side text as Category. This uses a simple nearest-Y match.
    """
    if df is None or df.empty or "__y" not in df.columns or not isinstance(lines, (list, tuple)):
        return df

    # cutoff for "left label" area: anything left of the first value column
    try:
        left_cutoff = min(col_xy.values()) - 24 if col_xy else float("inf")
    except Exception:
        left_cutoff = float("inf")

    # Gather (y_center, text) for left-side label words/lines
    label_pts: list[tuple[float, str]] = []
    for ln in lines:
        # be defensive about structures
        try:
            # case A: line is dict with 'words': [{'x','y','w','h','text'}, ...]
            words = ln.get("words") if isinstance(ln, dict) else None
            if words:
                left_text = []
                y_vals = []
                for w in words:
                    x = w.get("x", w.get("x0", None))
                    if x is None:
                        continue
                    if x < left_cutoff:
                        left_text.append(str(w.get("text", "")).strip())
                        y_vals.append(float(w.get("y", w.get("y0", 0.0))))
                if left_text and y_vals:
                    label_pts.append((sum(y_vals) / len(y_vals), " ".join(t for t in left_text if t)))
            else:
                # case B: line is simple string — ignore (no geometry)
                pass
        except Exception:
            continue

    if not label_pts:
        return df

    # simple nearest-neighbour on y
    ys = np.array([p[0] for p in label_pts])
    txt = [p[1] for p in label_pts]

    cats = []
    for y in df["__y"].tolist():
        try:
            idx = int(np.argmin(np.abs(ys - float(y))))
            if abs(ys[idx] - float(y)) <= y_tolerance:
                cats.append(txt[idx])
            else:
                cats.append("")
        except Exception:
            cats.append("")
    df["Category"] = [(_ or "").strip() for _ in cats]

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


# ========= CLI Workflow: Parse → LLM → Apply Ops → Export =========















"""Excel helpers have moved to pdf2excel.excel_io"""

# def _build_llm_payload(df: pd.DataFrame) -> dict:
#     """
#     Build the JSON the LLM needs: the table, columns, whitelist/synonyms,
#     and a 'suspects' list containing cells that should be derived.
#     """
#     if df is None or df.empty or "Category" not in df.columns:
#         return {"table": [], "columns": [], "suspects": [], "whitelist": [], "synonyms": {}}

#     table_for_llm = df.drop(columns=[c for c in df.columns if str(c).startswith("__")], errors="ignore").copy()
#     val_cols = [c for c in table_for_llm.columns if c != "Category"]
#     suspects = []

#     # rows the LLM can/should derive, with their component lists
#     TARGETS = {
#         "total non-operating gains (losses)": ["interest income, net", "loss on sale of assets", "donations (gift)"],
#         "net income (loss)": ["operating income", "total non-operating gains (losses)", "provision for income taxes"],
#         # (optional) add these if you want the LLM to also fill them:
#         # "total sales": ["client services revenue", "client service revenue", "book sales", "professional consultation"],
#         # "total expenses": ["wages", "wages and benefits", "marketing and advertising", "rent", "utilities",
#         #                    "memberships and publications", "insurance", "consultants", "office supplies"],
#     }

#     for i, row in table_for_llm.iterrows():
#         canon = _canon_label(row["Category"]).lower()
#         if canon in TARGETS:
#             for c in val_cols:
#                 if pd.isna(row[c]) or row[c] == "":
#                     suspects.append({
#                         "row": int(i),
#                         "col": str(c),
#                         "reason": "missing_total",
#                         "components": TARGETS[canon],
#                     })

#     payload = {
#         "table": table_for_llm.replace({np.nan: None}).to_dict(orient="records"),
#         "columns": table_for_llm.columns.tolist(),
#         "suspects": suspects,
#         "whitelist": sorted(list(WHITELIST)),
#         "synonyms": SYNONYMS,
#     }
#     return payload


# endregion

# region [S7] GUI (Tkinter)
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

# endregion

# region [S8] Pipeline (run_pipeline, run_gui, run_once_cli)
# -------------------- Main pipeline --------------------
MAX_VALUE_COLS = 5  # keep up to 5










NUM_RE = re.compile(r"^[()\d,.\-]+$")



HEADERISH_WORDS = {"ytd", "forecast"}
try:
    MONTH_TOKENS  # may already exist
except NameError:
    MONTH_TOKENS = {"jan","feb","mar","apr","may","jun","jul","aug","sep","sept","oct","nov","dec"}









"""Excel file I/O helpers now imported from pdf2excel.excel_io"""
# --- Header helpers ---------------------------------------------------------


# --- helper: normalize parse_finance_lines return shape to a DataFrame ---


# --- Header helpers (canonical) ---------------------------------------------






# --- main ------------------------------------------------------------------

# --- helpers (put near your other helpers) -----------------------------------







def _dump_debug_page(debug_dir: Path, page_idx: int, *, headers=None, lines=None, df_table=None, df_fallback=None):
    debug_dir.mkdir(parents=True, exist_ok=True)
    if headers is not None:
        (debug_dir / f"p{page_idx:02d}_headers.json").write_text(json.dumps(headers, indent=2))
    if lines is not None:
        try:
            (debug_dir / f"p{page_idx:02d}_lines.json").write_text(json.dumps(lines, indent=2))
        except Exception:
            pass
    if df_table is not None:
        try:
            df_table.to_csv(debug_dir / f"p{page_idx:02d}_grid.csv", index=False, encoding="utf-8-sig")
        except Exception:
            pass
    if df_fallback is not None:
        try:
            df_fallback.to_csv(debug_dir / f"p{page_idx:02d}_fallback.csv", index=False, encoding="utf-8-sig")
        except Exception:
            pass

# --- main pipeline (REPLACE your existing run_pipeline with this) ------------
def run_pipeline():
    try:
        if not selected_files:
            messagebox.showwarning("PDF → Excel", "Pick at least one file first.")
            return

        out = filedialog.asksaveasfilename(
            title="Save Excel as…",
            defaultextension=".xlsx",
            filetypes=[("Excel", "*.xlsx")],
            initialfile="extracted.xlsx",
        )
        if not out:
            return

        set_progress(0, 100, "Converting to images…")
        pages = pdf_or_images_to_pages(selected_files, dpi=300)
        if not pages:
            messagebox.showwarning("PDF → Excel", "No pages found.")
            return

        total_steps = len(pages) + 4
        all_tables = []

        for i, pimg in enumerate(pages, 1):
            set_progress(i-1, total_steps, f"OCR page {i}…")
            try:
                df_page = process_image(pimg, page_num=i)
            except Exception:
                df_page = None
            if df_page is not None and not df_page.empty:
                df_page["__page"] = i
                all_tables.append(df_page)

        if not all_tables:
            set_progress(total_steps, total_steps, "Done.")
            messagebox.showinfo("PDF → Excel", "No tables extracted.")
            return

        set_progress(len(pages), total_steps, "Merging…")
        merged = pd.concat(all_tables, ignore_index=True)

        # keep raw merged for LLM + finalize to handle cleanup in one place

        set_progress(len(pages)+1, total_steps, "LLM fixer…")
        with tempfile.TemporaryDirectory() as td:
            payload   = _build_llm_payload(merged)
            llm_edits = run_llm_fixer(payload, Path(td))

        merge_ops_fn = globals().get("_merge_llm_and_auto_ops")
        merged_ops = merge_ops_fn(merged, llm_edits) if callable(merge_ops_fn) else (llm_edits or {"ops": []})
        merged_fixed = apply_edit_script(merged.copy(), merged_ops)

        set_progress(len(pages)+2, total_steps, "Finalizing…")
        merged_fixed = finalize(merged_fixed)

        set_progress(len(pages)+3, total_steps, "Saving Excel…")
        if _is_file_locked(out):
            messagebox.showwarning(
                "File is open",
                "The output workbook is currently open in Excel.\n"
                "Close it (or choose a new name) to overwrite.\n\n"
                "I'll save to a timestamped filename instead.",
            )
        final_path = _safe_write_excel([merged_fixed], out)

        set_progress(total_steps, total_steps, "Done.")
        messagebox.showinfo("PDF → Excel", f"Saved:\n{final_path}")

        try:
            if sys.platform.startswith("win"):
                os.startfile(final_path)
            elif sys.platform == "darwin":
                subprocess.Popen(["open", final_path])
            else:
                subprocess.Popen(["xdg-open", final_path])
        except Exception:
            pass

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
def run_once_cli(input_path: str, output_path: str) -> str:
    pages = pdf_or_images_to_pages([Path(input_path)], dpi=300)
    all_dfs = []
    for i, pimg in enumerate(pages, 1):
        df = process_image(pimg)
        if df is not None and not getattr(df, "empty", True):
            df["__page"] = i
            all_dfs.append(df)
    if not all_dfs:
        raise RuntimeError("No tables extracted.")

    merged = pd.concat(all_dfs, ignore_index=True)
    # Defer cleanup to finalize()

    with tempfile.TemporaryDirectory() as td:
        payload   = _build_llm_payload(merged)
        llm_edits = run_llm_fixer(payload, Path(td))
    merged_ops = _merge_llm_and_auto_ops(merged, llm_edits)
    fixed = apply_edit_script(merged.copy(), merged_ops)
    fixed = finalize(fixed)

    saved = _safe_write_excel([fixed], output_path)
    return saved
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
        print(f"Saved: {saved}")

# endregion




