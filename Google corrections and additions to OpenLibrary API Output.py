"""
google_books_enricher_v2.py
───────────────────────────
Verify and enrich Dragonlance metadata using the Google Books API **without ever
modifying the original title string that came from your list** (now in the
“Requested Title” column). Instead, we capture Google’s canonical name in a new
column, **“GB Matched Title.”**

Run example:
    python google_books_enricher_v2.py \
        --input enriched_dragonlance_data.csv \
        --output enriched_dragonlance_data_google.csv

Notes
─────
* Compatible with the updated Open Library script that already keeps
  “Requested Title” + “Matched Title.”
* Adds columns only if they’re missing, so re-running is idempotent.
* Sleeps 1 s between calls by default to stay within API limits.
"""

from __future__ import annotations

import argparse
import csv
import os
import time
import requests
from typing import List, Optional, Dict

# --------------------------------------------------------------------------- #
#  Dragonlance fingerprints                                                   #
# --------------------------------------------------------------------------- #
DL_KEYWORDS: set[str] = {
    "dragonlance",
    "krynn",
    "ansalon",
    "solamnia",
    "draconian",
    "silvanesti",
    "qualinesti",
    "paladine",
    "takhisis",
    "raistlin",
    "fizban",
    "tasslehoff",
    "kender",
    "highlord",
    "weyr",
    "dragonarmy",
    "chronicles",
    "legends",
    "companions",
}

DL_AUTHORS: set[str] = {
    "Adam Lesh",
    "Amie Rose Rotruck",
    "Amy Stout",
    "Aron Eisenberg",
    "Barbara Siegel",
    "Brian Murphy",
    "Cam Banks",
    "Chris Pierson",
    "Christina Woods",
    "Dan Harnden",
    "Dan Parkinson",
    "Dan Willis",
    "Deborah Christian",
    "Don Perrin",
    "Donald Bingle",
    "Douglas Niles",
    "Douglas W. Clark",
    "Edo van Belkom",
    "Ellen Porath",
    "Fergus Ryan",
    "Giles Custer",
    "Harold Bakst",
    "J. Robert King",
    "Jake Bell",
    "Jamie Chambers",
    "Janet Pack",
    "Jean Rabe",
    "Jeff Crook",
    "Jeff Grubb",
    "Jeff Sampson",
    "John Grubber",
    "John Helfers",
    "Keith Baker",
    "Kevin Kage",
    "Kevin T. Stein",
    "Laura Hickman",
    "Linda P. Baker",
    "Lizz Weis",
    "Lucien Soulban",
    "Margaret Weis",
    "Mark Anthony",
    "Mark Sehestedt",
    "Mary H. Herbert",
    "Mary Kirchoff",
    "Michael Williams",
    "Mickey Zucker Reichert",
    "Miranda Horner",
    "Morris Simon",
    "Nancy Varian Berberick",
    "Nick O'Donohoe",
    "Paul B. Thompson",
    "Peter Archer",
    "R.D. Henham",
    "Rachel Gobar",
    "Rebecca Shelley",
    "Ree Soesbee",
    "Richard A. Knaak",
    "Robyn McGrew",
    "Roger E. Moore",
    "Roland J. Green",
    "Scott Haring",
    "Scott M. Buraczewski",
    "Scott Siegel",
    "Stan Brown",
    "Stephen D. Sullivan",
    "Steve Winter",
    "Sue Weinlein Cook",
    "Teri McLaren",
    "Teri Williams",
    "Tim Waggoner",
    "Tina Daniell",
    "Todd Fahnestock",
    "Tonya C. Cook",
    "Tracy Hickman",
    "Warren B. Smith",
    "William W. Connors",
}
DL_AUTHORS_LC = {a.lower() for a in DL_AUTHORS}

# --------------------------------------------------------------------------- #
#  Utility helpers                                                            #
# --------------------------------------------------------------------------- #

def looks_like_dragonlance_gb(volume: Dict) -> bool:
    """Heuristic: does a Google Books volume look like Dragonlance?"""
    vol = volume.get("volumeInfo", {})

    # Check author list first
    authors_lc = [a.lower() for a in vol.get("authors", [])]
    if any(a in DL_AUTHORS_LC for a in authors_lc):
        return True

    # Then keywords in categories or description
    text_blob = " ".join(vol.get("categories", []) + [vol.get("description", "")] ).lower()
    return any(kw in text_blob for kw in DL_KEYWORDS)


def extract_series_info(volume: Dict) -> tuple[str, str]:
    """Return (series_title, series_number) if we can parse them."""
    vol = volume.get("volumeInfo", {})
    series_title = series_num = ""

    # 1. Dedicated seriesInfo field (Google occasionally provides this)
    if "seriesInfo" in volume:
        s_info = volume["seriesInfo"]
        series_title = s_info.get("title", "") or series_title
        series_num = s_info.get("bookDisplayNumber", "") or series_num

    # 2. Parse patterns like "(Chronicles, Book 2)" from title/subtitle
    for field in ("title", "subtitle"):
        text = vol.get(field, "")
        if "(" in text and ")" in text:
            inner = text[text.find("(") + 1 : text.find(")")]
            if "," in inner:
                maybe_series, _, maybe_num = inner.partition(",")
                series_title = series_title or maybe_series.strip()
                series_num = series_num or maybe_num.strip()
            else:
                series_title = series_title or inner.strip()

    return series_title, series_num


def trunc(s: str, limit: int = 1000) -> str:
    """Truncate long strings for CSV."""
    return s if len(s) <= limit else s[: limit - 1] + "…"

# --------------------------------------------------------------------------- #
#  Google Books                                                               #
# --------------------------------------------------------------------------- #

def google_books_lookup(
    title: str,
    author: str = "",
    isbn: Optional[str] = None,
    api_key: Optional[str] = None,
) -> Optional[Dict]:
    """Return the best Google Books record matching our criteria."""
    if isbn:
        query = f"isbn:{isbn.split(',')[0].strip()}"
    else:
        q_parts: List[str] = [f'intitle:"{title}"']
        if author:
            first_author = author.split(',')[0].strip()
            if first_author:
                q_parts.append(f'inauthor:"{first_author}"')
        query = " ".join(q_parts)

    params = {"q": query, "maxResults": 5, "printType": "books"}
    if api_key:
        params["key"] = api_key

    resp = requests.get("https://www.googleapis.com/books/v1/volumes", params=params, timeout=15)
    if resp.status_code != 200:
        return None

    items = resp.json().get("items", [])
    return items[0] if items else None

# --------------------------------------------------------------------------- #
#  CSV ENRICHMENT                                                             #
# --------------------------------------------------------------------------- #

def enrich_csv(input_path: str, output_path: str, delay: float = 1.0) -> None:
    """Read <input_path>, add Google Books columns, write to <output_path>."""
    api_key = os.getenv("GOOGLE_BOOKS_API_KEY")

    with open(input_path, newline="", encoding="utf-8") as f_in, open(
        output_path, "w", newline="", encoding="utf-8"
    ) as f_out:
        reader = csv.DictReader(f_in)
        if reader.fieldnames is None:
            raise ValueError("Input CSV missing header row.")

        # Ensure Google columns exist
        fieldnames: List[str] = list(reader.fieldnames)
        for col in ("GB Matched Title", "GB Series", "GB Series #", "GB Summary"):
            if col not in fieldnames:
                fieldnames.append(col)

        writer = csv.DictWriter(f_out, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            requested_title = row.get("Requested Title") or row.get("Title") or ""
            author = row.get("Author(s)", "")
            isbn = row.get("ISBN", "") or None

            try:
                volume = google_books_lookup(requested_title, author, isbn, api_key)
                if volume and looks_like_dragonlance_gb(volume):
                    vinfo = volume["volumeInfo"]
                    row["GB Matched Title"] = vinfo.get("title", "")
                    series_title, series_num = extract_series_info(volume)
                    row["GB Series"] = series_title
                    row["GB Series #"] = series_num
                    row["GB Summary"] = trunc(vinfo.get("description", ""))
                else:
                    row["GB Matched Title"] = ""
                    row["GB Series"] = ""
                    row["GB Series #"] = ""
                    row["GB Summary"] = ""
            except Exception as exc:
                print(f"⚠️  {requested_title}: Google lookup failed → {exc}")
                row["GB Matched Title"] = "Error"
                row["GB Series"] = "Error"
                row["GB Series #"] = "Error"
                row["GB Summary"] = "Error"

            writer.writerow(row)
            time.sleep(delay)

# --------------------------------------------------------------------------- #
#  Get Er Done                                                                #
# --------------------------------------------------------------------------- #
if __name__=="__main__":
    p=argparse.ArgumentParser(description="Enhance Dragonlance CSV with Google Books data")
    p.add_argument("--input",default="enriched_dragonlance_data.csv")
    p.add_argument("--output",default="enriched_dragonlance_data_google.csv")
    p.add_argument("--sleep",type=float,default=1.0,help="Delay between API calls (seconds)")
    args=p.parse_args()
    enrich_csv(args.input,args.output,args.sleep)
    print(f"✅ Saved → {args.output}")