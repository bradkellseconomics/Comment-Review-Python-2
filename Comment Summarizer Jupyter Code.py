#!pip install python-dotenv

#!pip install numpy
#!pip install pandas

from openai import OpenAI
import os

# Load API keys from environment (.env supported if python-dotenv is installed)
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass  # Proceed if python-dotenv is not available

# regulations.gov API key
REGULATIONS_API_KEY = os.getenv("REGULATIONS_API_KEY")
API_BASE = "https://api.regulations.gov/v4"
HEADERS = {"X-Api-Key": REGULATIONS_API_KEY, "Accept": "application/json"} if REGULATIONS_API_KEY else {"Accept": "application/json"}

# OpenAI API key
open_ai_api_key = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=open_ai_api_key) if open_ai_api_key else None

#DOCKET
DOCKET_ID = "EPA-HQ-OPP-2008-0316"
MAX_PAGES = 1
PAGE_SIZE = 250
START_DATE = "2023-01-01"
END_DATE = "2026-01-01"

# =======================
# ðŸ“¦ Cell 1: Setup and Inputs
# =======================
#For later - class/config objects and global variables
import os
import time
import json
import requests
import numpy as np
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup
from datetime import datetime
from pdfminer.high_level import extract_text
from openai import OpenAI
from tqdm import tqdm
#from dotenv import load_dotenv, find_dotenv
import mimetypes

import warnings
import re as _re
import builtins as _builtins

# Quiet noisy warnings (e.g., deprecated endpoint chatter)
warnings.filterwarnings("ignore", category=DeprecationWarning)

# Sanitize stray mojibake characters from output
_REPLACEMENTS = [
    ("�?O", "[ERROR]"),
    ("�o.", "[OK]"),
    ("�o", "[OK]"),
    ("�s��,?", "[WARN]"),
    ("�s", "[WARN]"),
    ("ðŸ”Ž", ""),
]

def _sanitize_text(s: str) -> str:
    try:
        out = s
        for a, b in _REPLACEMENTS:
            out = out.replace(a, b)
        # Remove leading corrupted prefixes like dY… on a line
        out = _re.sub(r"^\s*dY.*?\s", "", out)
        # Drop stray replacement characters
        out = out.replace("�", "")
        return out
    except Exception:
        return s

def print(*args, **kwargs):  # type: ignore[override]
    cleaned = [( _sanitize_text(a) if isinstance(a, str) else a ) for a in args]
    return _builtins.print(*cleaned, **kwargs)

# Load environment variables from .env file
#load_dotenv(find_dotenv(filename="apis.env"))

#assert os.getenv("REGULATIONS_API_KEY"), "âŒ REGULATIONS_API_KEY not loaded!"
#assert os.getenv("OPENAI_API_KEY"), "âŒ OPENAI_API_KEY not loaded!"
#print("âœ… Environment variables loaded successfully.")

if client is None and open_ai_api_key:
    client = OpenAI(api_key=open_ai_api_key)

# === CONFIG ===

#API_BASE = "https://api.regulations.gov/v4"
#HEADERS = {
#    "X-Api-Key": os.getenv("REGULATIONS_API_KEY"),
#    "Accept": "application/json"
#}
#
#client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

OPENAI_MODEL = "gpt-4o"  # Use GPT-4 if you have API access

# Directory structure
BASE_DIR = os.path.join("data", DOCKET_ID)
ATTACH_DIR = os.path.join(BASE_DIR, "attachments")
HTML_DIR = os.path.join(BASE_DIR, "html_text")
META_DIR = os.path.join(BASE_DIR, "metadata")
PDF_TEXT_DIR = os.path.join(BASE_DIR, "pdf_text")
REFERENCE_TEXT_DIR = os.path.join(BASE_DIR, "reference_text")
CORE_IDEA_TXT_DIR = os.path.join(BASE_DIR, "core_summaries")
CORE_IDEA_XLSX = os.path.join(BASE_DIR, f"{DOCKET_ID}_core_ideas.xlsx")

# Create necessary directories
for path in [
    BASE_DIR, ATTACH_DIR, HTML_DIR, META_DIR,
    PDF_TEXT_DIR, REFERENCE_TEXT_DIR, CORE_IDEA_TXT_DIR
]:
    os.makedirs(path, exist_ok=True)

LOG_PATH = os.path.join(BASE_DIR, "log.txt")

def log(msg):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    full_msg = f"[{timestamp}] {_sanitize_text(str(msg))}"
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(full_msg + "\n")
    print(full_msg)

# =======================
# ðŸ“Œ Cell 2: Manual Attachment Downloader
# =======================

# =======================
# ðŸ“‚ Updated: HTML-based Attachment Downloader with Parallel Comment Processing
# =======================
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from concurrent.futures import ThreadPoolExecutor, as_completed


def scrape_attachments_from_html(comment_id):
    attachment_links = []
    try:
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        # Suppress DevTools listening and other noisy logs
        chrome_options.add_argument("--log-level=3")
        chrome_options.add_experimental_option('excludeSwitches', ['enable-logging', 'enable-automation'])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        # Suppress DevTools listening and other noisy logs
        chrome_options.add_argument("--log-level=3")
        chrome_options.add_experimental_option('excludeSwitches', ['enable-logging', 'enable-automation'])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        driver = webdriver.Chrome(options=chrome_options, service=Service(log_path=os.devnull))
        driver.set_page_load_timeout(30)

        url = f"https://www.regulations.gov/comment/{comment_id}"
        driver.get(url)

        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        time.sleep(5)

        links = driver.find_elements(By.XPATH, "//a[contains(@href, 'downloads.regulations.gov')]")
        for link in links:
            href = link.get_attribute("href")
            if "attachment" in href:
                attachment_links.append(href)

        driver.quit()
    except Exception as e:
        log(f"âŒ Failed to scrape attachments for {comment_id}: {e}")
        try:
            driver.quit()
        except:
            pass

    return attachment_links


def download_attachments_from_links(comment_id, links):
    downloaded_exts = []
    for url in links:
        ext = url.split(".")[-1].lower()
        filename = f"{comment_id}_{os.path.basename(url)}"
        path = os.path.join(ATTACH_DIR, filename)
        try:
            r = requests.get(url, timeout=10)
            if r.status_code == 200:
                with open(path, "wb") as f:
                    f.write(r.content)
                downloaded_exts.append(ext)
                log(f"âœ… Downloaded from HTML scrape: {filename}")
            else:
                log(f"âš ï¸ Attachment URL not downloadable (status {r.status_code}): {url}")
        except Exception as e:
            log(f"âŒ Failed to download attachment from HTML scrape: {e}")
    return downloaded_exts


# =======================
# âš™ï¸ Parallel Processing Helper for Comment IDs
# =======================
def process_comment_item(item):
    cid = item["id"]
    attr = item.get("attributes", {})
    date = attr.get("postedDate", "")
    api_submitter = attr.get("submitterName") or attr.get("organization") or ""
    api_html = attr.get("comment", "").strip()

    try:
        with open(os.path.join(META_DIR, f"{cid}.json"), "w", encoding="utf-8") as f:
            json.dump(item, f, indent=2)

        html_attachment_links = scrape_attachments_from_html(cid)
        attachments = download_attachments_from_links(cid, html_attachment_links)

        final_text = None
        submitter_line = None
        main_text = None

        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        driver = webdriver.Chrome(options=chrome_options, service=Service(log_path=os.devnull))
        driver.set_page_load_timeout(30)
        driver.get(f"https://www.regulations.gov/comment/{cid}")

        try:
            WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        except TimeoutException:
            log(f"âš ï¸ Timeout waiting for page body: {cid}")

        time.sleep(5)

        try:
            h1 = driver.find_element(By.TAG_NAME, "h1")
            submitter_line = h1.text.strip()
        except:
            submitter_line = None

        try:
            all_elements = driver.find_elements(By.XPATH, "//*[contains(text(), 'Comment')]")
            for el in all_elements:
                text = el.text.strip()
                if text.lower() == "comment":
                    parent = el.find_element(By.XPATH, "..")
                    siblings = parent.find_elements(By.XPATH, "*")
                    found_index = siblings.index(el)
                    for next_el in siblings[found_index+1:]:
                        maybe_comment = next_el.text.strip()
                        if maybe_comment:
                        #if maybe_comment and len(maybe_comment) > 100:
                            main_text = maybe_comment
                            break
                    if main_text:
                        break
        except Exception as e:
            log(f"âš ï¸ Structured comment block not found: {e}")

        driver.quit()
    except Exception as e:
        log(f"âŒ Error processing comment {cid}: {e}")
        try:
            driver.quit()
        except:
            pass
        return None

    if submitter_line and submitter_line.lower().startswith("comment submitted by"):
        submitter = submitter_line.split(" by ", 1)[-1].strip()
    else:
        submitter = submitter_line or api_submitter or "Anonymous"

    if any(attachments):
        strategy = "has_attachment"
        html_present = False
        final_text = None
    elif main_text:
        strategy = "use_scraped_html"
        html_present = True
        final_text = main_text
    elif api_html:
        strategy = "use_api_html"
        html_present = True
        final_text = api_html
    else:
        strategy = "blank"
        html_present = False
        final_text = None

    if not final_text and api_html:
        strategy = "fallback_api_html"
        final_text = api_html
        html_present = True

    if final_text:
        with open(os.path.join(HTML_DIR, f"{cid}.txt"), "w", encoding="utf-8") as f:
            f.write(final_text)

    return {
        "comment_id": cid,
        "date": date,
        "submitter": submitter,
        "has_html": html_present,
        "content_strategy": strategy,
        "attachment_1": attachments[0] if len(attachments) > 0 else None,
        "attachment_2": attachments[1] if len(attachments) > 1 else None,
        "attachment_3": attachments[2] if len(attachments) > 2 else None,
        "html_text": final_text or ""
    }



def download_comment_catalog(docket_id, max_pages=MAX_PAGES, start_date=START_DATE, end_date=END_DATE):
    page = 1
    rows = []

    while page <= max_pages:
        log(f"ðŸ”Ž Fetching page {page}")
        url = f"{API_BASE}/comments"
        params = {
            "filter[docketId]": docket_id,
            "page[size]": PAGE_SIZE,
            "page[number]": page,
            "sort": "-postedDate"
        }
        if start_date:
            params["filter[postedDate][ge]"] = start_date
        if end_date:
            params["filter[postedDate][le]"] = end_date

        try:
            resp = requests.get(url, headers=HEADERS, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            log(f"âŒ API error on page {page}: {e}")
            break

        comments = data.get("data", [])
        if not comments:
            log("âš ï¸ No more comments returned; stopping.")
            break

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(process_comment_item, item) for item in comments]
            for future in as_completed(futures):
                result = future.result()
                if result:
                    rows.append(result)

        page += 1
        time.sleep(0.5)

    df = pd.DataFrame(rows)
    comment_id_to_submitter = {
        str(row["comment_id"]): {
            "submitter": row.get("submitter", "Unknown"),
            }
        for _, row in df.iterrows()
    }

    log(f"âœ… Created in-memory map with {len(comment_id_to_submitter)} comment IDs.")

    summary_path = os.path.join(BASE_DIR, f"{docket_id}_summary.xlsx")
    df.to_excel(summary_path, index=False)
    log(f"âœ… Saved summary to: {summary_path}")



# =======================
# ðŸ“„ Cell 4: Extract Text from PDFs (and DOCX and TXT) with Cleaning
# =======================

import re
from pdfminer.high_level import extract_text

def clean_comment_text(text):
    """Clean up raw comment text for GPT processing."""
    
    # Step 1: Line-level cleaning
    lines = text.splitlines()
    clean_lines = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        if line.lower().startswith("page "):
            continue
        if "docket id" in line.lower():
            continue
        if re.fullmatch(r"\d{1,2}", line):
            continue

        # Remove common footer/address artifacts
        footer_patterns = [
            r"\d{4} .*? avma\.org",
            r"https?://\S+",
            r"^.*?Schaumburg.*?60173.*?$"
        ]
        if any(re.search(pat, line, flags=re.IGNORECASE) for pat in footer_patterns):
            continue

        clean_lines.append(line)

    # Step 2: Join and normalize internal formatting
    joined = "\n".join(clean_lines)
    
    # Replace mid-paragraph hard line breaks with spaces
    joined = re.sub(r'(?<!\n)\n(?!\n)', ' ', joined)

    # Normalize multiple blank lines to exactly one paragraph break
    joined = re.sub(r'\n\s*\n+', '\n\n', joined)

    return joined.strip()


def extract_attachment_texts():
    files = [f for f in os.listdir(ATTACH_DIR) if f.lower().endswith((".pdf", ".docx", ".txt"))]
    for fname in tqdm(files, desc="Extracting attachment text"):
        input_path = os.path.join(ATTACH_DIR, fname)
        base_name = os.path.splitext(fname)[0]
        output_path = os.path.join(PDF_TEXT_DIR, base_name + ".txt")

        try:
            if fname.lower().endswith(".pdf"):
                raw_text = extract_text(input_path)
            elif fname.lower().endswith(".docx"):
                from docx import Document
                doc = Document(input_path)
                raw_text = "\n".join([p.text for p in doc.paragraphs])
            elif fname.lower().endswith(".txt"):
                with open(input_path, "r", encoding="utf-8", errors="ignore") as f:
                    raw_text = f.read()
            else:
                continue

            cleaned_text = clean_comment_text(raw_text)

            if cleaned_text.strip():
                with open(output_path, "w", encoding="utf-8") as f:
                    f.write(cleaned_text)
                log(f"âœ… Extracted: {fname}")
            else:
                log(f"âš ï¸ Empty or unreadable: {fname}")

        except Exception as e:
            log(f"âŒ Failed to extract {fname}: {e}")

# =======================
# ðŸ”— Cell 5: Link Extracted Text Back to Summary
# =======================

def link_pdf_text_to_summary():
    summary_xlsx = os.path.join(BASE_DIR, f"{DOCKET_ID}_summary.xlsx")
    updated_xlsx = summary_xlsx.replace(".xlsx", "_with_pdfs.xlsx")

    df = pd.read_excel(summary_xlsx)
    file_map = {}

    for f in os.listdir(PDF_TEXT_DIR):
        if "_attachment_" in f and f.endswith(".txt"):
            cid = f.split("_attachment_")[0]
            rel_path = os.path.join("pdf_text", f)
            file_map.setdefault(cid, []).append(rel_path)

    # Ensure deterministic order
    for cid in file_map:
        file_map[cid].sort()

    df["extracted_attachment_texts"] = df["comment_id"].apply(
        lambda cid: "; ".join(file_map.get(cid, []))
    )

    # Add stakeholder classification columns
    is_major_list = []
    stakeholder_name_list = []
    stakeholder_type_list = []

    for _, row in tqdm(df.iterrows(), total=len(df), desc="Classifying stakeholders"):
        cid = row["comment_id"]
        submitter = row.get("submitter", "Unknown")

        text_paths = file_map.get(cid, [])
        full_text = ""
        for path in text_paths:
            try:
                with open(os.path.join(BASE_DIR, path), "r", encoding="utf-8") as f:
                    full_text += f.read() + "\n"
            except:
                continue

        is_major, stakeholder_name, stakeholder_type = gpt_stakeholder_classification(submitter, full_text)
        is_major_list.append(is_major)
        stakeholder_name_list.append(stakeholder_name)
        stakeholder_type_list.append(stakeholder_type)

    df["is_major_stakeholder"] = is_major_list
    df["stakeholder_name"] = stakeholder_name_list
    df["stakeholder_type"] = stakeholder_type_list
    
    updated_xlsx = summary_xlsx.replace(".xlsx", "_with_pdfs.xlsx")
    df.to_excel(updated_xlsx, index=False)
    log(f"ðŸ“Š Saved updated summary with attachment text links and stakeholder classification: {updated_xlsx}")

    # Optional: warn on missing text extractions
    missing = df[df["extracted_attachment_texts"] == ""]
    log(f"âš ï¸ Comments with no extracted text: {len(missing)}")

# =======================
# Cell 5.1 Stakeholder Classification
# =======================
import re

def strip_code_fence(text):
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?", "", text).strip()
        text = re.sub(r"```$", "", text).strip()
    return text

def gpt_stakeholder_classification(submitter_name, comment_excerpt):
    beginning = comment_excerpt[:1000] if comment_excerpt else ""
    end = comment_excerpt[-1000:] if comment_excerpt else ""

    prompt = f"""
Submitter name (from metadata): {submitter_name}

Excerpt from beginning of comment:
{beginning}

Excerpt from end of comment:
{end}

Based on these excerpts and the listed submitter, answer the following:
1. Is this a major stakeholder in this policy domain (yes or no)?
2. If yes, what is the most likely name of the stakeholder?
3. What is the stakeholder type? Choose from: government, academic, industry, non-profit, media, not-major-stakeholder, other.
Respond in JSON format with keys: major_stakeholder, stakeholder_name, stakeholder_type.
"""

    try:
        response = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {"role": "system", "content": "You classify public comment submitters in regulatory processes."},
                {"role": "user", "content": prompt.strip()}
            ],
            temperature=0,
            max_tokens=150
        )
        text = response.choices[0].message.content.strip()
        text = strip_code_fence(text)  # NEW: strip markdown code fences
        print("ðŸ§  Raw GPT response:\n", text)
        parsed = json.loads(text)
        return (
            str(parsed.get("major_stakeholder", "")).strip().lower() == "yes",
            parsed.get("stakeholder_name") or submitter_name or "Unknown",
            parsed.get("stakeholder_type", "other")
        )
    except Exception as e:
        log(f"âŒ GPT stakeholder classification failed for {submitter_name}: {e}")
        return (False, submitter_name or "Unknown", "other")

# ============================
# ðŸ“‹ Cell 5.2: Summarize Major Stakeholder Comments
# ============================

import os
import json
import pandas as pd
from openai import OpenAI

# === CONFIG ===
client = OpenAI(api_key=open_ai_api_key)
SUMMARY_INPUT_XLSX = os.path.join(BASE_DIR, f"{DOCKET_ID}_summary_with_pdfs.xlsx")
STAKEHOLDER_SUMMARY_XLSX = os.path.join(BASE_DIR, f"{DOCKET_ID}_major_stakeholder_summaries.xlsx")

# === GPT Prompt Template ===
SUMMARY_PROMPT_TEMPLATE = (
    "You are a regulatory analyst reviewing public comments from major stakeholders.\n\n"
    "Here is the full text of a stakeholder's comment submission.\n\n"
    "Please write a 3â€“5 sentence summary that captures the key concerns, positions, or themes raised in this comment.\n\n"
    "Comment Text:\n{text}"
)

# === GPT Summarization Function ===
def summarize_comment(text):
    if not text or len(text.strip()) < 100:
        return "(No meaningful text available for summarization.)"

    prompt = SUMMARY_PROMPT_TEMPLATE.format(text=text[:12000])
    try:
        response = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {"role": "system", "content": "You are a regulatory policy analyst."},
                {"role": "user", "content": prompt.strip()}
            ],
            temperature=0.4,
            max_tokens=500
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        return f"âŒ GPT summarization failed: {e}"

# === Main Function ===
def summarize_major_stakeholders():
    print("ðŸ“¥ Loading stakeholder-classified comments...")
    df = pd.read_excel(SUMMARY_INPUT_XLSX)

    major_df = df[df["is_major_stakeholder"] == True].copy()
    print(f"ðŸ” Found {len(major_df)} major stakeholder comments")

    summary_rows = []
    for _, row in major_df.iterrows():
        cid = row["comment_id"]
        name = row.get("stakeholder_name", "Unknown")
        stype = row.get("stakeholder_type", "other")
        submitter = row.get("submitter", "Unknown")

        # Load full text of comment
        full_text = ""
        for path in str(row.get("extracted_attachment_texts", "")).split("; "):
            if path:
                try:
                    with open(os.path.join(BASE_DIR, path), "r", encoding="utf-8") as f:
                        full_text += f.read() + "\n"
                except:
                    continue

        print(f"ðŸ§  Summarizing comment {cid} from {name}...")
        summary = summarize_comment(full_text)

        summary_rows.append({
            "comment_id": cid,
            "submitter": submitter,
            "stakeholder_name": name,
            "stakeholder_type": stype,
            "summary": summary
        })

    pd.DataFrame(summary_rows).to_excel(STAKEHOLDER_SUMMARY_XLSX, index=False)
    print(f"âœ… Saved major stakeholder summaries to {STAKEHOLDER_SUMMARY_XLSX}")


# ============================
# ðŸ“‹ Build Canonical Themes (Simplified) â€” with debug preview
# ============================

import os
import json
import pandas as pd
from openai import OpenAI

# === CONFIG ===
client = OpenAI(api_key=open_ai_api_key)
SUMMARY_INPUT_XLSX = os.path.join(BASE_DIR, f"{DOCKET_ID}_summary_with_pdfs.xlsx")
CANONICAL_THEMES_JSON = os.path.join(BASE_DIR, f"{DOCKET_ID}_canonical_themes.json")

# === Prompt Templates ===
CANDIDATE_THEME_PROMPT = (
    "You are reviewing a major stakeholder's public comment in a federal rulemaking.\n\n"
    "Extract a short list (3â€“6) of ISSUE THEMES. Each theme should be a short phrase (â‰¤5 words)\n"
    "that could be used to group similar claims from different commenters. Examples: 'Visa duration limits', 'SEVIS tracking', 'Program change restrictions'.\n\n"
    "Return ONLY a JSON array of strings.\n"
    "Comment Text:\n{text}"
)

MERGE_THEMES_PROMPT = (
    "You are consolidating issue themes from multiple major stakeholder submissions.\n\n"
    "You will be given a list of candidate themes. Merge overlapping or synonymous ones, and produce a final canonical list of 10â€“20 themes.\n"
    "Each canonical theme should be â‰¤5 words, consistent, and reusable for grouping.\n\n"
    "Return ONLY a JSON array of strings.\n\n"
    "Candidate Themes:\n{themes}"
)

def extract_candidate_themes(text: str):
    """Extract candidate themes from a single stakeholder submission."""
    if not text.strip():
        return []
    prompt = CANDIDATE_THEME_PROMPT.format(text=text[:12000])
    try:
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {"role": "system", "content": "You are an expert regulatory analyst."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,
            max_tokens=500
        )
        raw = resp.choices[0].message.content.strip()
        return json.loads(raw.strip('`').replace('json', '').strip())
    except Exception as e:
        print(f"âŒ Theme extraction failed: {e}")
        return []

def build_canonical_themes(max_reviews: int = 10):
    print("ðŸ“¥ Loading stakeholder-classified commentsâ€¦")
    df = pd.read_excel(SUMMARY_INPUT_XLSX)

    major_df = df[df.get("is_major_stakeholder", False) == True].copy()
    if len(major_df) > max_reviews:
        major_df = major_df.head(max_reviews)

    all_candidates = []
    for i, row in enumerate(major_df.itertuples(index=False), 1):
        submitter = getattr(row, "submitter", "Unknown")
        comment_id = getattr(row, "comment_id", "Unknown")
        text_paths = str(getattr(row, "extracted_attachment_texts", "")).split("; ")

        # Load + concatenate text
        full_text = ""
        for path in text_paths:
            if path:
                try:
                    with open(os.path.join(BASE_DIR, path), "r", encoding="utf-8") as f:
                        full_text += f.read() + "\n"
                except Exception:
                    continue

        preview = (full_text[:100] or "").replace("\n", " ").replace("\r", " ")
        if not preview:
            preview = "<EMPTY>"

        print(f"\nðŸ§­ Major submission #{i}")
        print(f"   â€¢ Stakeholder: {submitter}")
        print(f"   â€¢ Comment ID: {comment_id}")
        print(f"   â€¢ Preview (100 chars): {preview}")

        text_paths = str(getattr(row, "extracted_attachment_texts", "")).split("; ")
        full_text = ""
        for path in text_paths:
            if path:
                try:
                    with open(os.path.join(BASE_DIR, path), "r", encoding="utf-8") as f:
                        full_text += f.read() + "\n"
                except:
                    continue

        # ðŸ‘‡ Add this single line right after building full_text:
        print(f"[{getattr(row, 'comment_id', 'UNKNOWN')}] looking for text in: {text_paths or '<none>'} | length={len(full_text)} | preview={full_text[:80]!r}")


        candidates = extract_candidate_themes(full_text)
        print(f"   â€¢ Extracted themes: {len(candidates)}"
              f"{'  âš ï¸ (ZERO THEMES)' if len(candidates) == 0 else ''}")

        # Optional: show the themes briefly (first 6) for spot-checking
        if candidates:
            print(f"   â€¢ First themes: {', '.join(map(str, candidates[:6]))}")

        all_candidates.extend(candidates)

    print(f"\nðŸ“¦ Total candidate themes collected: {len(all_candidates)}")

    # === Merge into canonical set ===
    merge_prompt = MERGE_THEMES_PROMPT.format(themes=json.dumps(all_candidates, indent=2))
    try:
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {"role": "system", "content": "You are an expert in consolidating regulatory themes."},
                {"role": "user", "content": merge_prompt}
            ],
            temperature=0.3,
            max_tokens=800
        )
        raw = resp.choices[0].message.content.strip()
        canonical_themes = json.loads(raw.strip('`').replace('json', '').strip())
    except Exception as e:
        print(f"âŒ Canonical merge failed: {e}")
        canonical_themes = []

    with open(CANONICAL_THEMES_JSON, "w", encoding="utf-8") as f:
        json.dump(canonical_themes, f, indent=2)

    print(f"âœ… Wrote canonical themes â†’ {CANONICAL_THEMES_JSON} (n={len(canonical_themes)})")
    return canonical_themes


   
# =======================
# ðŸ§  Cell 6: Comment Deduplication & Template Grouping (Enhanced for Stakeholder and Importance Awareness)
# =======================

import os
import pandas as pd
from rapidfuzz import fuzz
from collections import defaultdict
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE

# === CONFIG ===
TEXT_DIR = os.path.join(BASE_DIR, "pdf_text")  # fallback to html_text if no pdf
ALT_TEXT_DIR = os.path.join(BASE_DIR, "html_text")
SUMMARY_WITH_PDFS_XLSX = os.path.join(BASE_DIR, f"{DOCKET_ID}_summary_with_pdfs.xlsx")
FINAL_OUTPUT_XLSX = os.path.join(BASE_DIR, f"{DOCKET_ID}_final_comment_summary.xlsx")
REFERENCE_TEXT_DIR = os.path.join(BASE_DIR, "reference_text")
SIMILARITY_THRESHOLD = 90
os.makedirs(REFERENCE_TEXT_DIR, exist_ok=True)

# === Helper ===
def clean_excel_text(text):
    return ILLEGAL_CHARACTERS_RE.sub("", text)

# === Step 1: Load comment text ===
def load_comment_text():
    comments = {}

    for fname in os.listdir(TEXT_DIR):
        if fname.endswith(".txt"):
            comment_attachment_id = fname.replace(".txt", "")
            comment_id = comment_attachment_id.split("_attachment_")[0]
            with open(os.path.join(TEXT_DIR, fname), "r", encoding="utf-8", errors="ignore") as f:
                comments[comment_attachment_id] = {
                    "comment_id": comment_id,
                    "text": f.read().strip()
                }

    for fname in os.listdir(ALT_TEXT_DIR):
        if fname.endswith(".txt"):
            comment_attachment_id = fname.replace(".txt", "")
            comment_id = comment_attachment_id.split("_attachment_")[0]
            if comment_attachment_id not in comments:
                with open(os.path.join(ALT_TEXT_DIR, fname), "r", encoding="utf-8", errors="ignore") as f:
                    comments[comment_attachment_id] = {
                        "comment_id": comment_id,
                        "text": f.read().strip()
                    }

    return comments

# === Step 2: Deduplicate ===
def deduplicate_comments(comments):
    template_id_counter = 1
    assigned = {}
    templates = {}
    reference_texts = {}

    for attachment_id, data in comments.items():
        text = data["text"]
        matched = False
        for tid, example_ids in templates.items():
            ref_id = example_ids[0]
            ref_text = comments[ref_id]["text"]
            sim = fuzz.token_sort_ratio(text, ref_text)
            if sim >= SIMILARITY_THRESHOLD:
                assigned[attachment_id] = tid
                templates[tid].append(attachment_id)
                matched = True
                break

        if not matched:
            tid = f"T{template_id_counter:03}"
            templates[tid] = [attachment_id]
            assigned[attachment_id] = tid
            reference_texts[tid] = text
            template_id_counter += 1

    return templates, reference_texts, assigned

# === Step 3: Merge with summary and save ===
def merge_templates_into_summary(templates, reference_texts, assigned, comments):
    rows = []
    df = pd.read_excel(SUMMARY_WITH_PDFS_XLSX)
    if "submitter" not in df.columns:
        log("âš ï¸ No submitter column found in summary. Adding placeholder.")
        df["submitter"] = "Unknown"

    df["comment_id"] = df["comment_id"].astype(str)

    template_rows = []
    for tid, attachment_ids in templates.items():
        ref_attachment_id = attachment_ids[0]
        comment_id = comments[ref_attachment_id]["comment_id"]
        ref_text = reference_texts[tid]
        ref_path = os.path.join(REFERENCE_TEXT_DIR, f"{ref_attachment_id}.txt")
        with open(ref_path, "w", encoding="utf-8") as f:
            f.write(ref_text)

        group_df = df[df["comment_id"] == comment_id]
        stakeholders = group_df["is_major_stakeholder"].fillna(False)
        stakeholder_names = "; ".join(sorted(group_df[stakeholders]["submitter"].dropna().unique()))

        importance_summary = {}
        if "importance" in group_df.columns:
            importance_summary = group_df["importance"].value_counts().to_dict()

        template_rows.append({
            "template_id": tid,
            "reference_comment_id": comment_id,
            "reference_comment_attachment_id": ref_attachment_id,
            "num_comments": len(attachment_ids),
            "is_mass_mailer": len(attachment_ids) > 1,
            "comment_attachment_ids": ", ".join(attachment_ids),
            "reference_path": ref_path,
            "contains_stakeholder": stakeholders.any(),
            "stakeholder_names": stakeholder_names,
            "submitter": group_df["submitter"].iloc[0] if not group_df["submitter"].empty else "Unknown",
            "importance_summary": str(importance_summary)
        })

    template_df = pd.DataFrame(template_rows)
    merged = df.merge(template_df, left_on="comment_id", right_on="reference_comment_id", how="right")
    merged.to_excel(FINAL_OUTPUT_XLSX, index=False)
    log(f"âœ… Final merged summary saved: {FINAL_OUTPUT_XLSX}")

    
# ==========================
# âœ¨ Cell 6.1: Comment_ID to submitter map
# ==========================

def build_comment_id_to_submitter_map(summary_xlsx_path):
    """
    Build a dictionary that maps comment_id to submitter and is_major_stakeholder flag.

    Parameters:
        summary_xlsx_path (str): Path to the summary Excel file with comment metadata

    Returns:
        dict: {
            comment_id: {
                "submitter": str,
                "is_major_stakeholder": bool
            }
        }
    """
    df = pd.read_excel(summary_xlsx_path)
    comment_id_to_submitter = {}

    for _, row in df.iterrows():
        comment_id = str(row.get("comment_id", "")).strip()
        comment_id_to_submitter[comment_id] = {
            "submitter": row.get("submitter", "Unknown"),
            "is_major_stakeholder": row.get("is_major_stakeholder", False)
        }

    return comment_id_to_submitter  
 



# ============================
# âœ¨ Cell 7: Extract Core Claims to Canonical Schema (1 row per claim)
# âœ… Uses live-loaded canonical themes in the prompt
# âœ… Minimal schema (no claim/justification) + keeps label for singletons
# ============================

import os
import json
import re
from collections import defaultdict

import pandas as pd
from tqdm import tqdm
from openai import OpenAI

# === CONFIG ===
REFERENCE_TEXT_DIR = os.path.join(BASE_DIR, "reference_text")
CLAIM_OUTPUT_XLSX = os.path.join(BASE_DIR, f"{DOCKET_ID}_core_claims.xlsx")
CLAIM_OUTPUT_TXT_DIR = os.path.join(BASE_DIR, "core_claims")
CANONICAL_THEMES_JSON_PATH = os.path.join(BASE_DIR, f"{DOCKET_ID}_canonical_themes.json")

# === OpenAI Client ===
client = OpenAI(api_key=open_ai_api_key)

# === Minimal Extraction Prompt (with label for singletons) ===
EXTRACTION_INSTRUCTION_TMPL = (
    "You are analyzing a public comment submitted to a federal agency during a rulemaking process.\n\n"
    "Use ONLY the text provided in this chunk. If there is no concrete policy ask/objection, return an empty array [].\n\n"
    "Task: extract structured policy feedback. For each DISTINCT policy issue in the text, return an object with:\n"
    "- clusterable_claim: â‰¤12 words, start with 'Supports', 'Opposes', or 'Requests', followed by a clear policy action or requirement.\n"
    "  Example: 'Opposes fixed four-year visa term' or 'Requests extension of grace period to 60 days'.\n"
    "  The object must describe the regulatory action or requirement itself (not the rationale or effect).\n"
    "  Avoid vague nouns like 'policy', 'rule', or 'proposal' unless the comment truly refers to the entire rule.\n"
    "  If no clear policy ask exists, set to 'Not stated.'\n"
    "- issue_theme: ONE EXACT string from the canonical list below, or 'Other'.\n"
    "- source_quote: â‰¤250 characters of direct quote that best evidences the ask/stance.\n"
    "- position: one of 'support' | 'oppose' | 'support with modifications' | 'request clarification' | 'not stated'.\n"
    "- importance: 'High' | 'Medium' | 'Low' based on novelty/clarity/specificity/relevance.\n"
    "- label: â‰¤6 words, Title Case, that captures the core ask or stance (e.g., 'Oppose Four-Year Limit').\n"
    "- summary: 2â€“3 sentences (plain language) summarizing the ask and its scope (no fluff, no invented facts).\n\n"
    "Canonical issue themes (exact strings):\n{themes_json}\n\n"
    "Guidelines:\n"
    "- Split distinct policy issues; do not paraphrase canonical theme names; focus on the regulatory ask.\n"
    "- Keep clusterable_claim literal and embedding-friendly (short Verb + Object only).\n"
    "- Prefer canonical nouns when possible (e.g., 'visa term' instead of 'program duration').\n"
    "- label should be short and recognizable at a glance by policymakers.\n"
    "- source_quote must be verbatim and â‰¤250 characters.\n"
    "- Respond with ONLY valid JSON (an array of objects). No prose.\n"
)

SYSTEM_MSG = (
    "You extract minimal, auditable argument units from public comments. "
    "Use only the provided text; if there is no concrete ask, return []."
)


def chunk_by_sections(text, max_chunk_size=10000, overlap_chars=300):
    sections = [s.strip() for s in text.split('\n\n') if s.strip()]
    chunks, current = [], ""
    for section in sections:
        if len(section) > max_chunk_size:
            start = 0
            while start < len(section):
                part = section[start:start+max_chunk_size]
                if current.strip():
                    chunks.append(current.strip())
                    current = ""
                chunks.append(part.strip())
                start += max_chunk_size - overlap_chars
            continue
        if len(current) + len(section) + 2 <= max_chunk_size:
            current += section + "\n\n"
        else:
            if current.strip():
                chunks.append(current.strip())
            if overlap_chars > 0 and len(current) > overlap_chars:
                overlap = current[-overlap_chars:]
                current = overlap + section + "\n\n"
            else:
                current = section + "\n\n"
    if current.strip():
        chunks.append(current.strip())
    return chunks

def parse_gpt_json(text):
    """Strip markdown fences and parse JSON output from GPT."""
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?", "", text, flags=re.IGNORECASE).strip()
        text = re.sub(r"```$", "", text).strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        print("âš ï¸ JSON parsing failed:", e)
        return []

# --- Optional normalization helpers ---
_ALLOWED_POS = {"support","oppose","support with modifications","request clarification","not stated"}
def _clean(s): 
    return (s or "").strip()
def _norm_pos(s):
    s = _clean(s).lower()
    return s if s in _ALLOWED_POS else s  # keep as-is if modelâ€™s a bit off; fix later if needed
def _cap1(s):
    return s[:1].upper() + s[1:] if s else s

# === Claim Extraction Function (canonical schema rows) ===
def extract_all_claims():
    os.makedirs(CLAIM_OUTPUT_TXT_DIR, exist_ok=True)
    all_rows = []

    # Load canonical themes fresh
    try:
        with open(CANONICAL_THEMES_JSON_PATH, "r", encoding="utf-8") as f:
            _themes_list = json.load(f)
    except FileNotFoundError:
        print(f"âš ï¸ Canonical themes file not found: {CANONICAL_THEMES_JSON_PATH}. Using empty list.")
        _themes_list = []
    except Exception as e:
        print(f"âš ï¸ Failed to load canonical themes: {e}. Using empty list.")
        _themes_list = []

    themes_json = json.dumps(_themes_list, indent=2)
    print("Using canonical themes (first 5):", _themes_list[:5])

    # Per-file claim counters for stable unit_ids
    claim_counter_by_attachment = defaultdict(int)

    for fname in tqdm(sorted(os.listdir(REFERENCE_TEXT_DIR)), desc="Extracting claims"):
        if not fname.endswith(".txt"):
            continue

        comment_attachment_id = fname.replace(".txt", "")
        comment_id = comment_attachment_id.split("_attachment_")[0]
        path = os.path.join(REFERENCE_TEXT_DIR, fname)
        with open(path, "r", encoding="utf-8") as f:
            full_text = f.read()

        chunks = chunk_by_sections(full_text)

        for chunk in chunks:
            try:
                prompt = EXTRACTION_INSTRUCTION_TMPL.format(themes_json=themes_json) + "\n\nComment:\n" + chunk
                response = client.chat.completions.create(
                    model=OPENAI_MODEL,
                    messages=[
                        {"role": "system", "content": SYSTEM_MSG},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.2,
                    max_tokens=1500
                )
                issues = parse_gpt_json(response.choices[0].message.content.strip())

                for entry in issues:
                    # increment counter and build a stable claim unit_id
                    claim_counter_by_attachment[comment_attachment_id] += 1
                    idx = claim_counter_by_attachment[comment_attachment_id]
                    unit_id = f"{comment_attachment_id}#c{idx}"

                    clusterable_claim = _clean(entry.get("clusterable_claim"))
                    issue_theme       = _clean(entry.get("issue_theme"))
                    position          = _norm_pos(entry.get("position"))
                    importance        = _cap1(_clean(entry.get("importance")))
                    label             = _clean(entry.get("label")) or clusterable_claim
                    summary           = _clean(entry.get("summary")) or clusterable_claim
                    source_quote      = _clean(entry.get("source_quote"))

                    all_rows.append({
                        # identity & typing
                        "unit_type": "claim",
                        "unit_id": unit_id,

                        # theme & stance
                        "issue_theme": issue_theme,
                        "position": position,
                        "importance": importance,

                        # actors
                        "is_major_stakeholder": comment_id_to_submitter.get(comment_id, {}).get("is_major_stakeholder", False),
                        "submitter": comment_id_to_submitter.get(comment_id, {}).get("submitter", "Unknown"),

                        # content (clusterable + human-facing)
                        "clusterable_claim": clusterable_claim,   # short, clusterable form
                        "label": label,                  # title-style label (used for singletons too)
                        "summary": summary,              # 1â€“2 sentence claim summary

                        # membership
                        "comment_ids": json.dumps([comment_attachment_id]),
                        "n_members": 1,
                        "parent_id": "",

                        # clustering slots to be filled later (steps 2â€“4)
                        "cluster_id_orig": "",
                        "cluster_id_final": "",
                        "theme_cluster_id_orig": "",
                        "theme_cluster_id": "",
                        "nearest_units": "",

                        # traceability
                        "algo_version": f"extract_v1_{OPENAI_MODEL}",
                        "comment_id": comment_id,
                        "comment_attachment_id": comment_attachment_id,
                        "source_quote": source_quote,
                    })

            except Exception as e:
                # Emit a row that flags the error but still conforms to canonical shape
                claim_counter_by_attachment[comment_attachment_id] += 1
                idx = claim_counter_by_attachment[comment_attachment_id]
                unit_id = f"{comment_attachment_id}#c{idx}"

                all_rows.append({
                    "unit_type": "claim",
                    "unit_id": unit_id,
                    "issue_theme": "[ERROR]",
                    "position": "",
                    "importance": "",
                    "is_major_stakeholder": comment_id_to_submitter.get(comment_id, {}).get("is_major_stakeholder", False),
                    "submitter": comment_id_to_submitter.get(comment_id, {}).get("submitter", "Unknown"),
                    "clusterable_claim": "[ERROR]",
                    "label": "[ERROR]",
                    "summary": f"[ERROR] {e}",
                    "comment_ids": json.dumps([comment_attachment_id]),
                    "n_members": 1,
                    "parent_id": "",
                    "cluster_id_orig": "",
                    "cluster_id_final": "",
                    "theme_cluster_id_orig": "",
                    "theme_cluster_id": "",
                    "nearest_units": "",
                    "algo_version": f"extract_v1_{OPENAI_MODEL}",
                    "comment_id": comment_id,
                    "comment_attachment_id": comment_attachment_id,
                    "source_quote": "[ERROR]",
                })

    df = pd.DataFrame(all_rows)

    # Reorder to keep canonical fields prominent
    canonical_cols = [
        "unit_type", "unit_id", "issue_theme", "position", "importance",
        "is_major_stakeholder", "submitter",
        "clusterable_claim", "label", "summary",
        "comment_ids", "n_members", "parent_id",
        "cluster_id_orig", "cluster_id_final", "theme_cluster_id_orig", "theme_cluster_id", "nearest_units",
        "algo_version",
    ]
    extras = [c for c in df.columns if c not in canonical_cols]
    df = df[canonical_cols + extras]

    df.to_excel(CLAIM_OUTPUT_XLSX, index=False)
    print(f"âœ… Saved extracted claims to: {CLAIM_OUTPUT_XLSX}")


# ============================
# Cell 8: Minimal, Explainable Pipeline
#   Step 1: embed_claims(...)
#   Step 2: cluster_within_theme(...)
#   Step 3: meta_cluster_within_theme(...)
#   Step 4: k-nn clustering
# ============================

import os
import numpy as np
import pandas as pd
import hdbscan
import openai
from sklearn.preprocessing import normalize
from sklearn.metrics.pairwise import cosine_similarity

# --- Config (edit as needed) ---
EMBED_MODEL       = "text-embedding-3-large"
MIN_CLUSTER_SIZE  = 2
SENTINEL_START    = 100000   # unique ids for noise/singletons
BATCH_SIZE        = 64       # embedding batching
META_SIM_THRESHOLD= 0.75     # cosine similarity for merging clusters

# ---------- OpenAI client ----------
def _client(api_key: str):
    if not api_key:
        raise ValueError("OPENAI API key is required.")
    return openai.OpenAI(api_key=api_key)

# ---------- STEP 1: Embedding ----------
def embed_claims(
    input_xlsx: str,
    output_index_xlsx: str,
    output_embeddings_npy: str,
    open_ai_api_key: str,
    text_col: str = "clusterable_claim",   # or "clusterable_claim" if you prefer
    theme_col: str = "issue_theme"
):
    """
    Read input_xlsx (must contain text_col and theme_col).
    Write:
      - output_index_xlsx: exact row order + minimal metadata
      - output_embeddings_npy: np.ndarray of embeddings aligned to the index sheet
    """
    print("ðŸ“¥ Loading:", input_xlsx)
    df = pd.read_excel(input_xlsx)

    if text_col not in df.columns or theme_col not in df.columns:
        raise ValueError(f"Expected columns: {text_col}, {theme_col}")

    texts = df[text_col].fillna("").astype(str).tolist()

    print(f"ðŸ§  Embedding {len(texts)} rows with {EMBED_MODEL}â€¦")
    client = _client(open_ai_api_key)

    vecs = []
    for i in range(0, len(texts), BATCH_SIZE):
        chunk = texts[i:i+BATCH_SIZE]
        resp = client.embeddings.create(model=EMBED_MODEL, input=chunk)
        vecs.extend([d.embedding for d in resp.data])
    E = np.array(vecs, dtype=np.float32)

    # Save outputs
    idx_df = df.copy()  # keep original columns so later steps can use them
    idx_df.reset_index(drop=True, inplace=True)
    idx_df.to_excel(output_index_xlsx, index=False)
    np.save(output_embeddings_npy, E)

    print(f"ðŸ’¾ Saved index to: {output_index_xlsx}")
    print(f"ðŸ’¾ Saved embeddings to: {output_embeddings_npy}")
    return output_index_xlsx, output_embeddings_npy


# ---------- helpers used by steps 2 & 3 ----------
def _compute_centroids(norm_vectors: np.ndarray, labels: np.ndarray):
    """Return {label: centroid_vector}."""
    cents = {}
    for lbl in np.unique(labels):
        idx = np.where(labels == lbl)[0]
        if len(idx) > 0:
            cents[int(lbl)] = norm_vectors[idx].mean(axis=0)
    return cents

def _compute_exemplars(texts, norm_vectors: np.ndarray, labels: np.ndarray):
    """
    Exemplar = item closest to its cluster centroid (for clusters with size>=2).
    For singletons (sentinels), exemplar is that single item.
    Returns {label: (row_index, text)}.
    """
    ex = {}
    cents = _compute_centroids(norm_vectors, labels)
    for lbl, cvec in cents.items():
        idx = np.where(labels == lbl)[0]
        if len(idx) == 1:
            i = idx[0]
            ex[int(lbl)] = (i, texts[i])
        else:
            sims = (norm_vectors[idx] @ (cvec / (np.linalg.norm(cvec) + 1e-12)))
            best = idx[int(np.argmax(sims))]
            ex[int(lbl)] = (best, texts[best])
    return ex

def _closest_clusters_from_centroids(centroids: dict, k: int = 3) -> dict:
    """
    For each cluster label, return the top-k nearest other cluster labels by cosine.
    """
    if not centroids:
        return {}
    labels = list(centroids.keys())
    M = np.stack([centroids[l] for l in labels])
    S = cosine_similarity(M)
    out = {}
    for i, lbl in enumerate(labels):
        order = np.argsort(S[i])[::-1]
        top = []
        for j in order:
            if labels[j] != lbl:
                top.append(str(labels[j]))
            if len(top) >= k:
                break
        out[lbl] = ", ".join(top)
    return out


# ---------- STEP 2: Clustering within theme ----------
def cluster_within_theme(
    index_xlsx: str,
    embeddings_npy: str,
    output_xlsx: str,
    text_col: str = "clusterable_claim",
    theme_col: str = "issue_theme",
):
    """
    Load the index sheet + embeddings; for each theme:
      - L2 normalize embeddings for cosine-equivalent clustering
      - HDBSCAN with min_cluster_size=MIN_CLUSTER_SIZE
      - Noise points (-1) get unique sentinel ids >= SENTINEL_START
      - Compute cluster centroids / exemplars / sizes / closest clusters
      - ASSERT: no real cluster (id < SENTINEL_START) has size < MIN_CLUSTER_SIZE
    Write a single Excel with columns:
      theme, cluster_id_orig, theme_cluster_id_orig, closest_centroid_clusters, cluster_size, cluster_exemplar, etc.
    """
    print("ðŸ“¥ Loading:", index_xlsx, "and", embeddings_npy)
    df = pd.read_excel(index_xlsx)
    E = np.load(embeddings_npy)
    assert len(df) == len(E), "index sheet and embeddings length mismatch"

    texts = df[text_col].fillna("").astype(str).tolist()
    themes = df[theme_col].fillna("").astype(str)

    # normalize once
    X = normalize(E.astype(float))

    labels_all = np.full(len(df), fill_value=np.nan)
    cluster_size_all = np.full(len(df), fill_value=np.nan)
    cluster_exemplar_all = np.empty(len(df), dtype=object)
    closest_map_all = np.empty(len(df), dtype=object)

    next_singleton = SENTINEL_START

    for theme, idxs in themes.groupby(themes).groups.items():
        idxs = np.array(sorted(list(idxs)))
        if len(idxs) == 0:
            continue
        if len(idxs) == 1:
            # HDBSCAN needs >= 2 to form any cluster. Treat as noise -> sentinel.
            labels = np.array([-1], dtype=int)
        else:
            clusterer = hdbscan.HDBSCAN(min_cluster_size=MIN_CLUSTER_SIZE, metric="euclidean")
            labels = clusterer.fit_predict(X[idxs]).astype(int)

        # map noise to unique sentinel ids
        neg = np.where(labels == -1)[0]
        if len(neg):
            labels = labels.copy()
            for j, pos in enumerate(neg):
                labels[pos] = next_singleton + j
            next_singleton += len(neg)

        # sanity: check cluster sizes for real ids (< SENTINEL_START)
        real_ids, counts = np.unique(labels[labels < SENTINEL_START], return_counts=True)
        if len(real_ids):
            min_size = counts.min()
            assert min_size >= MIN_CLUSTER_SIZE, \
                f"Found cluster with size {min_size} (<{MIN_CLUSTER_SIZE}) in theme '{theme}'."

        # compute centroids/exemplars/nearest among clusters in this theme
        cents = _compute_centroids(X[idxs], labels)
        ex = _compute_exemplars([texts[i] for i in idxs], X[idxs], labels)
        near = _closest_clusters_from_centroids(cents, k=3) if len(cents) > 1 else {}

        # write back
        labels_all[idxs] = labels
        sizes = {int(lbl): int((labels == lbl).sum()) for lbl in np.unique(labels)}
        cluster_size_all[idxs] = [sizes[int(lbl)] for lbl in labels]
        cluster_exemplar_all[idxs] = [ex[int(lbl)][1] for lbl in labels]
        closest_map_all[idxs] = [near.get(int(lbl), "") for lbl in labels]

    df["cluster_id_orig"] = labels_all.astype(int)
    df["theme_cluster_id_orig"] = df[theme_col].astype(str) + "::" + df["cluster_id_orig"].astype(str)
    df["cluster_size"] = cluster_size_all.astype(int)
    df["cluster_exemplar"] = cluster_exemplar_all
    df["closest_centroid_clusters"] = closest_map_all

    print("ðŸ’¾ Writing:", output_xlsx)
    df.to_excel(output_xlsx, index=False)
    print("âœ… Saved per-theme clusters to:", output_xlsx)
    return output_xlsx


def meta_cluster_within_theme(
    clustered_xlsx: str,
    embeddings_npy: str,
    output_xlsx: str,
    theme_col: str = "issue_theme",
):
    print("ðŸ“¥ Loading:", clustered_xlsx, "and", embeddings_npy)
    df = pd.read_excel(clustered_xlsx)
    E = np.load(embeddings_npy)
    assert len(df) == len(E), "cluster sheet and embeddings length mismatch"

    X = normalize(E.astype(float))
    labels_orig = df["cluster_id_orig"].astype(int).to_numpy()
    labels_final = labels_orig.copy()

    for theme, idxs in df.groupby(theme_col).groups.items():
        idxs = np.array(sorted(list(idxs)))
        if len(idxs) <= 1:
            continue

        theme_labels = labels_final[idxs]
        uniq = np.unique(theme_labels)
        if len(uniq) <= 1:
            continue

        # ---- build centroids for all clusters in this theme (incl. singletons)
        cents = {}
        for lbl in uniq:
            rows = idxs[theme_labels == lbl]
            cents[int(lbl)] = X[rows].mean(axis=0)

        keys = list(cents.keys())
        M = np.stack([cents[k] for k in keys])
        S = cosine_similarity(M)

        # ---- union-find merge by META_SIM_THRESHOLD
        parent = {k: k for k in keys}
        def find(x):
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x
        def union(a, b):
            ra, rb = find(a), find(b)
            if ra != rb:
                parent[max(ra, rb)] = min(ra, rb)

        for i, ki in enumerate(keys):
            for j in range(i + 1, len(keys)):
                kj = keys[j]
                if S[i, j] >= META_SIM_THRESHOLD:
                    union(ki, kj)

        # ---- apply merge to THIS THEME ONLY
        remap = {k: find(k) for k in keys}
        theme_final = theme_labels.copy()
        for lbl in keys:
            theme_final[theme_labels == lbl] = remap[lbl]
        labels_final[idxs] = theme_final

        # ---- promote â€œnoiseâ€ sentinel ids (>= SENTINEL_START) that now have size â‰¥ 2
        SENTINEL_START = 100000
        counts = pd.Series(theme_final).value_counts()
        real_mask = theme_final < SENTINEL_START
        max_real = int(theme_final[real_mask].max()) if real_mask.any() else -1
        next_real = max_real + 1

        for old_id, cnt in counts.items():
            if old_id >= SENTINEL_START and cnt >= 2:
                theme_final = np.where(theme_final == old_id, next_real, theme_final)
                next_real += 1

        # write promoted labels back to the master array for this theme
        labels_final[idxs] = theme_final

    # ---- after all themes processed, write once
    df["cluster_id_final"] = labels_final.astype(int)
    df["theme_cluster_id"] = df[theme_col].astype(str) + "::" + df["cluster_id_final"].astype(str)

    # ---- recompute metadata per theme using final labels
    cluster_size = np.full(len(df), np.nan)
    cluster_exemplar = np.empty(len(df), dtype=object)
    closest_map = np.empty(len(df), dtype=object)
    txt_col = "clusterable_claim" 
    texts = df[txt_col].fillna("").astype(str).tolist()

    for theme, idxs in df.groupby(theme_col).groups.items():
        idxs = np.array(sorted(list(idxs)))
        tlabels = df.loc[idxs, "cluster_id_final"].astype(int).to_numpy()
        cents = _compute_centroids(X[idxs], tlabels)
        ex = _compute_exemplars([texts[i] for i in idxs], X[idxs], tlabels)
        near = _closest_clusters_from_centroids(cents, k=3) if len(cents) > 1 else {}

        sizes = {int(lbl): int((tlabels == lbl).sum()) for lbl in np.unique(tlabels)}
        cluster_size[idxs] = [sizes[int(lbl)] for lbl in tlabels]
        cluster_exemplar[idxs] = [ex[int(lbl)][1] for lbl in tlabels]
        closest_map[idxs] = [near.get(int(lbl), "") for lbl in tlabels]

    df["cluster_size"] = cluster_size.astype(int)
    df["cluster_exemplar"] = cluster_exemplar
    df["closest_centroid_clusters"] = closest_map

    print("ðŸ’¾ Writing:", output_xlsx)
    df.to_excel(output_xlsx, index=False)
    print("âœ… Saved meta-clustered results (including singletons) to:", output_xlsx)
    return output_xlsx


# ---------- STEP 4: k-NN attach pass (within theme, after meta-clustering) ----------
def attach_singletons_knn_within_theme(
    clustered_xlsx: str,            # output from Step 3 (meta_cluster_within_theme)
    embeddings_npy: str,            # same embeddings array used in Steps 2â€“3
    output_xlsx: str,               # write updated labels + metadata here
    theme_col: str = "issue_theme",
    attach_threshold: float = 0.7, # cosine threshold to attach a singleton
    min_target_size: int = 2,       # only attach into clusters with size >= this
    use_centroids: bool = True,     # True = singletonâ†’centroid; False = singletonâ†’nearest point
):
    """
    For each theme:
      - Identify singletons (clusters of size==1) in cluster_id_final
      - Find nearest existing *multi-member* cluster within the same theme
      - If cosine sim >= attach_threshold, reassign the singleton into that cluster
      - Recompute cluster_size/exemplar/closest_centroid_clusters and theme_cluster_id
    """
    import numpy as np
    import pandas as pd
    from sklearn.preprocessing import normalize
    from sklearn.metrics.pairwise import cosine_similarity

    print("ðŸ“¥ Loading:", clustered_xlsx, "and", embeddings_npy)
    df = pd.read_excel(clustered_xlsx)
    E = np.load(embeddings_npy)
    assert len(df) == len(E), "sheet and embeddings length mismatch"

    # ---- Snapshot postâ€“meta-cluster labels for traceability ----
    if "cluster_id_meta" not in df.columns:
        df["cluster_id_meta"] = df["cluster_id_final"]
    if "theme_cluster_id_meta" not in df.columns:
        # if theme_cluster_id isn't present yet, build it from current cluster_id_final
        if "theme_cluster_id" in df.columns:
            df["theme_cluster_id_meta"] = df["theme_cluster_id"]
        else:
            df["theme_cluster_id_meta"] = (
                df[theme_col].astype(str) + "::" + df["cluster_id_final"].astype(int).astype(str)
            )

    
    # L2-normalized vectors for cosine math
    X = normalize(E.astype(float))

    # Work on an int array copy of final labels
    labels_final = pd.to_numeric(df["cluster_id_final"], errors="coerce").fillna(-1).astype(int).to_numpy()

    # Process per theme to keep memory bounded
    for theme, idxs in df.groupby(theme_col).groups.items():
        idxs = np.array(sorted(list(idxs)))
        if len(idxs) <= 1:
            continue

        # Theme slice
        labels_t = labels_final[idxs]
        X_t = X[idxs]

        # Count sizes
        uniq, counts = np.unique(labels_t, return_counts=True)
        size_map = {int(u): int(c) for u, c in zip(uniq, counts)}

        # Identify singletons (size==1)
        single_mask = np.array([size_map[l] == 1 for l in labels_t])
        single_idx_local = np.where(single_mask)[0]
        if len(single_idx_local) == 0:
            continue  # nothing to attach

        # Candidates to attach into: clusters with size >= min_target_size
        keep_labels = np.array([u for u, c in size_map.items() if c >= min_target_size], dtype=int)
        if len(keep_labels) == 0:
            continue  # no eligible targets

        # Build nearest target (either centroids or all member points)
        if use_centroids:
            # centroid of each eligible cluster
            cents = {}
            for lbl in keep_labels:
                mem_local = np.where(labels_t == lbl)[0]
                cents[lbl] = X_t[mem_local].mean(axis=0)
            cent_labels = np.array(sorted(cents.keys()), dtype=int)
            C = normalize(np.vstack([cents[l] for l in cent_labels]))
            Xs = X_t[single_idx_local]                     # (s, d)
            sims = Xs @ C.T                                # (s, C)
            best_j = sims.argmax(axis=1)
            best_sim = sims[np.arange(len(single_idx_local)), best_j]
            best_target_labels = cent_labels[best_j]
        else:
            # nearest individual point among all eligible targets
            target_rows_local = np.where(np.isin(labels_t, keep_labels))[0]
            Xa = X_t[target_rows_local]                    # (m, d)
            Xs = X_t[single_idx_local]                     # (s, d)
            sims = Xs @ Xa.T                               # (s, m)
            # prevent self-match in the (rare) case a singletonâ€™s own label is also â€œeligibleâ€
            for r, i_loc in enumerate(single_idx_local):
                # zero out its own column if it slipped in (shouldnâ€™t if size==1 and min_target_size>=2)
                pass
            nn_j_local = sims.argmax(axis=1)
            best_sim = sims[np.arange(len(single_idx_local)), nn_j_local]
            best_target_labels = labels_t[target_rows_local[nn_j_local]]

        # Apply attachments if above threshold
        changed = 0
        for k, i_loc in enumerate(single_idx_local):
            sim = float(best_sim[k])
            target_lbl = int(best_target_labels[k])
            if sim >= attach_threshold:
                old_lbl = int(labels_t[i_loc])
                if old_lbl != target_lbl:
                    labels_t[i_loc] = target_lbl
                    changed += 1

        if changed:
            labels_final[idxs] = labels_t
            print(f"ðŸ”— {theme}: attached {changed} singleton(s) to nearest clusters (â‰¥ {attach_threshold:.2f})")

    # Write updated labels back
    df["cluster_id_final"] = labels_final.astype(int)
    df["theme_cluster_id"] = df[theme_col].astype(str) + "::" + df["cluster_id_final"].astype(str)

    # ---- Recompute metadata (size, exemplar, closest) per theme ----
    cluster_size = np.full(len(df), fill_value=np.nan)
    cluster_exemplar = np.empty(len(df), dtype=object)
    closest_map = np.empty(len(df), dtype=object)
    # choose the same text col you used earlier
    text_col = "clusterable_claim" 
    texts = df[text_col].fillna("").astype(str).tolist()

    def _compute_centroids(norm_vectors: np.ndarray, labels: np.ndarray):
        cents = {}
        for lbl in np.unique(labels):
            idx = np.where(labels == lbl)[0]
            if len(idx):
                cents[int(lbl)] = norm_vectors[idx].mean(axis=0)
        return cents

    def _compute_exemplars(texts_local, X_local: np.ndarray, labels_local: np.ndarray):
        ex = {}
        cents = _compute_centroids(X_local, labels_local)
        for lbl, cvec in cents.items():
            idx = np.where(labels_local == lbl)[0]
            if len(idx) == 1:
                i = idx[0]
                ex[int(lbl)] = (i, texts_local[i])
            else:
                c = cvec / (np.linalg.norm(cvec) + 1e-12)
                sims = X_local[idx] @ c
                best = idx[int(np.argmax(sims))]
                ex[int(lbl)] = (best, texts_local[best])
        return ex

    def _closest_clusters_from_centroids(centroids: dict, k: int = 3) -> dict:
        if not centroids:
            return {}
        keys = list(centroids.keys())
        M = np.stack([centroids[k] for k in keys])
        S = cosine_similarity(M)
        out = {}
        for i, lbl in enumerate(keys):
            order = np.argsort(S[i])[::-1]
            nbrs = [str(keys[j]) for j in order if keys[j] != lbl][:k]
            out[lbl] = ", ".join(nbrs)
        return out

    for theme, idxs in df.groupby(theme_col).groups.items():
        idxs = np.array(sorted(list(idxs)))
        labels_t = df.loc[idxs, "cluster_id_final"].astype(int).to_numpy()
        X_t = X[idxs]
        cents = _compute_centroids(X_t, labels_t)
        ex = _compute_exemplars([texts[i] for i in idxs], X_t, labels_t)
        near = _closest_clusters_from_centroids(cents, k=3) if len(cents) > 1 else {}

        sizes = {int(lbl): int((labels_t == lbl).sum()) for lbl in np.unique(labels_t)}
        cluster_size[idxs] = [sizes[int(lbl)] for lbl in labels_t]
        cluster_exemplar[idxs] = [ex[int(lbl)][1] for lbl in labels_t]
        closest_map[idxs] = [near.get(int(lbl), "") for lbl in labels_t]

    df["cluster_size"] = cluster_size.astype(int)
    df["cluster_exemplar"] = cluster_exemplar
    df["closest_centroid_clusters"] = closest_map

    print("ðŸ’¾ Writing:", output_xlsx)
    df.to_excel(output_xlsx, index=False)
    print("âœ… Saved k-NN attach results to:", output_xlsx)
    return output_xlsx


# ============================
# ðŸ·ï¸ Cell 9: Cluster Labeling & Summarization (Schema v1, aligned)
# - Skips GPT for singletons (population-1 clusters and noise singletons)
# - Uses only clusterable_claim/label/summary (no claim/justification)
# - Adds summary_source: 'gpt-sampled' | 'passthrough-singleton' | 'passthrough-noise'
# ============================

import os
import re
import numpy as np
import pandas as pd
from openai import OpenAI

EMBEDDED_XLSX      = os.path.join(BASE_DIR, f"{DOCKET_ID}_embedding_clusters_meta_attached.xlsx")
THEME_SUMMARY_XLSX = os.path.join(BASE_DIR, f"{DOCKET_ID}_cluster_theme_summaries.xlsx")

SENTINEL_START = 100000
SAMPLE_SEED    = 42

auto_client = OpenAI(api_key=open_ai_api_key)

THEME_PROMPT_TEMPLATE = (
    "You are a regulatory analyst summarizing grouped public comments.\n\n"
    "Each group below is a SAMPLE from a larger cluster of semantically similar claims.\n"
    "Your task is to:\n"
    "1) Provide a short, informative label that captures the central policy ask/stance (avoid rationale in the label).\n"
    "2) Write a 2â€“3 sentence summary that generalizes across the sampled claims.\n"
    "3) If the sample shows distinct sub-themes, add up to 3 short bullets.\n\n"
    "Rules:\n"
    "- Use only the provided sample; do NOT invent facts.\n"
    "- If coverage is limited, write a cautious summary.\n"
    "- Use EXACTLY this format (one label, one summary block):\n"
    "  Label: <short label>\n"
    "  Summary: <2â€“3 sentences; optional bullets>\n\n"
    "Sample size: {sample_size} of {cluster_size} (coverage: {cover_rate})\n\n"
    "Sampled structured data:\n\n"
    "{claims}"
)

# ---------- helpers ----------

def _clean_str(x) -> str:
    if pd.isna(x):
        return ""
    s = str(x).strip()
    return "" if s.lower() in ("nan", "none", "null") else s

def parse_label_summary(text: str):
    label_match = re.search(r"Label:\s*(.*)", text)
    summary_match = re.search(r"Summary:\s*(.*)", text, re.DOTALL)
    label = label_match.group(1).strip() if label_match else ""
    summary = summary_match.group(1).strip() if summary_match else text.strip()
    return label, summary

def _pick_exemplar_row(cluster_df: pd.DataFrame) -> pd.Series:
    if "cluster_exemplar" in cluster_df.columns:
        ex_val = str(cluster_df["cluster_exemplar"].iloc[0]).strip()
        if ex_val:
            hits = cluster_df[cluster_df["clusterable_claim"].astype(str).str.strip() == ex_val]
            if len(hits):
                return hits.iloc[0]
    return cluster_df.iloc[0]

def _sample_cluster_rows(cluster_df: pd.DataFrame,
                         max_per_cluster:int,
                         max_majors:int,
                         seed:int = SAMPLE_SEED) -> tuple[pd.DataFrame, dict]:
    rng = np.random.default_rng(seed)
    exemplar = _pick_exemplar_row(cluster_df)
    taken = {exemplar.name}

    majors = cluster_df[(cluster_df.get("is_major_stakeholder", False) == True) & (~cluster_df.index.isin(taken))].copy()
    majors["submitter_key"] = majors.get("submitter", "").astype(str).str.strip()
    majors_groups = [g for _, g in majors.groupby("submitter_key")]
    majors_pool = [g.iloc[0] for g in majors_groups]
    if len(majors_pool) > max_majors:
        idxs = rng.choice(len(majors_pool), size=max_majors, replace=False)
        majors_pick = [majors_pool[i] for i in idxs]
    else:
        majors_pick = majors_pool
    taken |= set([row.name for row in majors_pick])

    remaining = max(0, max_per_cluster - (1 + len(majors_pick)))
    fillers = cluster_df[~cluster_df.index.isin(taken)].copy()
    random_pick = []
    if remaining > 0 and not fillers.empty:
        fillers["submitter_key"] = fillers.get("submitter", "").astype(str).str.strip()
        fillers = fillers.sample(frac=1.0, random_state=int(seed))
        seen = set()
        for _, r in fillers.iterrows():
            key = r["submitter_key"]
            if key in seen:
                continue
            random_pick.append(r)
            seen.add(key)
            if len(random_pick) >= remaining:
                break

    sampled_rows = [exemplar] + majors_pick + random_pick
    sampled_df = pd.DataFrame(sampled_rows).drop_duplicates()

    meta = {
        "sample_size": len(sampled_df),
        "cluster_size": len(cluster_df),
        "cover_rate": round(len(sampled_df) / max(1, len(cluster_df)), 3)
    }
    return sampled_df, meta

def _importance_summary_text(series: pd.Series) -> str:
    vals = (
        series.fillna("")
        .astype(str).str.strip().str.capitalize()
        .replace({"": "Unspecified", "Nan": "Unspecified"})
    )
    counts = vals.value_counts()
    parts = [f"{v} {k}" for k, v in counts.items()]
    return ", ".join(parts)

def run_gpt_theme_summary(sample_df: pd.DataFrame, meta: dict) -> str:
    # Only use the aligned minimal fields
    claims_text = "\n\n".join(
        f"Issue Theme: {row.get('issue_theme','')}\n"
        f"Position: {row.get('position','')}\n"
        f"Claim: {row.get('clusterable_claim','')}\n"
        f"Importance: {row.get('importance','')}"
        for _, row in sample_df.iterrows()
    )
    prompt = THEME_PROMPT_TEMPLATE.format(
        sample_size=meta["sample_size"],
        cluster_size=meta["cluster_size"],
        cover_rate=meta["cover_rate"],
        claims=claims_text[:11000]
    )
    resp = auto_client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=[
            {"role": "system", "content": "You are a public policy analyst."},
            {"role": "user",   "content": prompt}
        ],
        temperature=0.4,
        max_tokens=800
    )
    return resp.choices[0].message.content.strip()

def get_sampling_knobs(cluster_size: int):
    if cluster_size < 5:
        return 3, 1
    if cluster_size < 20:
        return 5, 2
    elif cluster_size < 50:
        return 7, 3
    elif cluster_size < 100:
        return 9, 4
    else:
        return 12, 5


# ============================
# Main entrypoint
# ============================

def summarize_clusters_sampled():
    print("ðŸ“¥ Loading clustered claimsâ€¦", EMBEDDED_XLSX)
    df = pd.read_excel(EMBEDDED_XLSX)

    # Required columns (aligned schema)
    needed = [
        "theme_cluster_id","cluster_id_final","cluster_exemplar","closest_centroid_clusters",
        "issue_theme","position","clusterable_claim","label","summary",
        "importance","comment_id","submitter","is_major_stakeholder"
    ]
    for col in needed:
        if col not in df.columns:
            df[col] = np.nan

    # Clean/normalize
    for col in ["theme_cluster_id","clusterable_claim","label","summary",
                "importance","closest_centroid_clusters","submitter","issue_theme"]:
        df[col] = df[col].apply(_clean_str)
    df["cluster_id_final"] = pd.to_numeric(df["cluster_id_final"], errors="coerce")

    # Keep rows with valid final cluster id + theme
    df_valid = df[df["cluster_id_final"].notna() & (df["theme_cluster_id"] != "")].copy()

    # True population per final theme_cluster_id
    pop_sizes = df_valid.groupby("theme_cluster_id").size().rename("pop_size")
    df_valid = df_valid.merge(pop_sizes, left_on="theme_cluster_id", right_index=True, how="left")

    # Split sets
    is_noise = df_valid["cluster_id_final"] >= SENTINEL_START
    singletons_noise = df_valid[is_noise].copy()                        # noise singletons
    singletons_pop1  = df_valid[~is_noise & (df_valid["pop_size"] == 1)].copy()  # population-1 clusters
    clustered        = df_valid[~is_noise & (df_valid["pop_size"] > 1)].copy()   # multi-member clusters

    summary_rows = []

    # ---- Multi-member clusters (GPT) ----
    for tcid in sorted(clustered["theme_cluster_id"].unique()):
        cluster_df = clustered[clustered["theme_cluster_id"] == tcid].copy()
        issue_theme = _clean_str(cluster_df["issue_theme"].iloc[0])
        num_claims      = int(len(cluster_df))
        num_commenters  = int(cluster_df["comment_id"].nunique())
        contains_major  = bool(cluster_df.get("is_major_stakeholder", False).fillna(False).any())
        importance_summary = _importance_summary_text(cluster_df["importance"])
        submitters_all = "; ".join(sorted(cluster_df["submitter"].dropna().astype(str).unique()))
        stakeholder_submitters = "; ".join(sorted(
            cluster_df.loc[cluster_df["is_major_stakeholder"]==True, "submitter"]
            .dropna().astype(str).unique()
        ))
        stakeholder_count = int(cluster_df["is_major_stakeholder"].fillna(False).sum())
        comment_ids = "; ".join(sorted(cluster_df["comment_id"].dropna().astype(str).unique()))

        max_per_cluster, max_majors = get_sampling_knobs(num_claims)
        sample_df, meta = _sample_cluster_rows(
            cluster_df,
            max_per_cluster=max_per_cluster,
            max_majors=max_majors,
            seed=SAMPLE_SEED
        )

        print(f"ðŸ§  Summarizing cluster {tcid} (sample {meta['sample_size']}/{meta['cluster_size']}, cov={meta['cover_rate']})")
        label, summary = parse_label_summary(run_gpt_theme_summary(sample_df, meta))

        closest_str = "; ".join(sorted(set(map(str, cluster_df["closest_centroid_clusters"].dropna().unique()))))

        summary_rows.append({
            "theme_cluster_id": tcid,
            "issue_theme": issue_theme,
            "submitters_all": submitters_all,
            "submitters_stakeholders": stakeholder_submitters,
            "stakeholder_count": stakeholder_count,
            "comment_ids": comment_ids,
            "cluster_n": num_claims,
            "commenters": num_commenters,
            "contains_stakeholder": contains_major,
            "importance_summary": importance_summary,
            "label": _clean_str(label),
            "summary": _clean_str(summary),
            "closest_clusters": closest_str,
            "sample_size": meta["sample_size"],
            "cluster_size": meta["cluster_size"],
            "coverage": meta["cover_rate"],
            "submitter_sample": "; ".join(sorted(sample_df.get("submitter","Unknown").dropna().astype(str).unique())),
            "summary_source": "gpt-sampled"
        })

    # ---- Passthrough helpers for singletons (no GPT) ----
    def _passthrough_row(row: pd.Series, source_tag: str):
        tcid    = _clean_str(row.get("theme_cluster_id", ""))
        issue_theme = _clean_str(row.get("issue_theme", ""))
        # Prefer extracted label/summary; fall back to claim/theme
        label   = _clean_str(row.get("label", "")) or _clean_str(row.get("clusterable_claim", "")) or issue_theme
        summary = _clean_str(row.get("summary", "")) or _clean_str(row.get("clusterable_claim","")) or issue_theme
        imp_val = _clean_str(row.get("importance", "")) or "Unspecified"
        importance_summary = f"1 {imp_val.capitalize()}"
        is_major = bool(row.get("is_major_stakeholder", False))
        submitter_all = _clean_str(row.get("submitter", "Unknown"))
        comment_ids = _clean_str(row.get("comment_id", "Unknown"))
        stakeholder_submitters = submitter_all if is_major else ""
        stakeholder_count = 1 if is_major else 0

        return {
            "theme_cluster_id": tcid,
            "issue_theme": issue_theme,
            "submitters_all": submitter_all,
            "submitters_stakeholders": stakeholder_submitters,
            "stakeholder_count": stakeholder_count,
            "comment_ids": comment_ids,
            "cluster_n": 1,
            "commenters": 1,
            "contains_stakeholder": bool(row.get("is_major_stakeholder", False)),
            "importance_summary": importance_summary,
            "label": label,
            "summary": summary,
            "closest_clusters": _clean_str(row.get("closest_centroid_clusters", "")),
            "sample_size": 1,
            "cluster_size": 1,
            "coverage": 1.0,
            "submitter_sample": submitter_all,
            "summary_source": source_tag
        }

    # ---- Singletons: passthrough (no GPT) ----
    # Only include those with High importance
    # (guard against NaNs before .str)
    df_imp = df_valid.copy()
    df_imp["importance"] = df_imp["importance"].fillna("").astype(str)
    high_importance_singletons = singletons_pop1[df_imp.loc[singletons_pop1.index, "importance"].str.lower() == "high"].copy()
    high_importance_noise      = singletons_noise[df_imp.loc[singletons_noise.index,  "importance"].str.lower() == "high"].copy()

    for _, r in high_importance_singletons.iterrows():
        summary_rows.append(_passthrough_row(r, "passthrough-singleton"))

    for _, r in high_importance_noise.iterrows():
        summary_rows.append(_passthrough_row(r, "passthrough-noise"))

    out_df = pd.DataFrame(summary_rows)
    out_df.to_excel(THEME_SUMMARY_XLSX, index=False)
    print(f"âœ… Saved cluster labels & summaries to {THEME_SUMMARY_XLSX}")

# How to run:
# summarize_clusters_sampled()






download_comment_catalog(DOCKET_ID)


print("done")





extract_attachment_texts()


link_pdf_text_to_summary()




# === Run the pipeline ===
comments = load_comment_text()
templates, reference_texts, assigned = deduplicate_comments(comments)
merge_templates_into_summary(templates, reference_texts, assigned, comments)



#summarize_major_stakeholders()


canonical_themes = build_canonical_themes(max_reviews=10)

print("Canonical themes:")
for theme in canonical_themes:
    print(" -", theme)



































comment_id_to_submitter = build_comment_id_to_submitter_map(SUMMARY_WITH_PDFS_XLSX)
extract_all_claims()







# Paths (adjust to your BASE_DIR/DOCKET_ID)
INPUT_XLSX   = os.path.join(BASE_DIR, f"{DOCKET_ID}_core_claims.xlsx")

EMB_INDEX_XL = os.path.join(BASE_DIR, f"{DOCKET_ID}_emb_index.xlsx")
EMB_NPY      = os.path.join(BASE_DIR, f"{DOCKET_ID}_embeddings.npy")

CLUSTERS_XL  = os.path.join(BASE_DIR, f"{DOCKET_ID}_embedding_clusters.xlsx")
META_XL      = os.path.join(BASE_DIR, f"{DOCKET_ID}_embedding_clusters_meta.xlsx")

# 1) Embedding
embed_claims(
    input_xlsx=INPUT_XLSX,
    output_index_xlsx=EMB_INDEX_XL,
    output_embeddings_npy=EMB_NPY,
    open_ai_api_key=open_ai_api_key,
    text_col="clusterable_claim",     
    theme_col="issue_theme"
)




# 2) Clustering within theme
cluster_within_theme(
    index_xlsx=EMB_INDEX_XL,
    embeddings_npy=EMB_NPY,
    output_xlsx=CLUSTERS_XL,
    text_col="clusterable_claim",
    theme_col="issue_theme"
)


# 3) Meta-clustering within theme (merge near-duplicates)
meta_cluster_within_theme(
    clustered_xlsx=CLUSTERS_XL,
    embeddings_npy=EMB_NPY,
    output_xlsx=META_XL,
    theme_col="issue_theme"
)


# 4) K-NN clustering
META_XL = os.path.join(BASE_DIR, f"{DOCKET_ID}_embedding_clusters_meta.xlsx")
EMB_NPY = os.path.join(BASE_DIR, f"{DOCKET_ID}_embeddings.npy")
ATTACH_XL = os.path.join(BASE_DIR, f"{DOCKET_ID}_embedding_clusters_meta_attached.xlsx")

attach_singletons_knn_within_theme(
    clustered_xlsx=META_XL,
    embeddings_npy=EMB_NPY,
    output_xlsx=ATTACH_XL,
    theme_col="issue_theme",
    attach_threshold=0.75,  # tune 0.70â€“0.75
    min_target_size=2,      # only attach into true clusters
    use_centroids=True      # switch to False to use nearest-point instead of centroid
)


print("done")



# === Run the function ===
summarize_clusters_sampled()

#Also add a function to icnrease the number of claims reviewed as the order of magnitude of claims in each cluster increases









#Review clusters first by looking at clusters then at singletons.
#add more detail to summaries


#Modify clusters to only be among "high" importance comments (drop medium before finding closest)
    #Kinda done. dropping all unmatched singletons that aren't "high"
#Individually summarize all comments from major stakeholders
    #Done
#fix count issue for second cluster
    #Kinda done - removed second cluster

#I need to change the above to not run GPT on singletons - revise earlier summary code to write up output sufficient for here.
    #DONE

    
#Train on other RTCs?
#Give the NPRM to it for a sense of what it is responding to?

#Add the ability to run multiple times and not re-review comments it already has reviewed?

#modify canonical theme selection to use major commenters of different "types"











