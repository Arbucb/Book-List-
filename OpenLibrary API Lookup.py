"""
Fetch Dragonlance book metadata from Open Library.

Given a plain-text file of titles, the script writes a CSV containing only the
matching Dragonlance works even when identically titled, non-Dragonlance books
exist.

Updates (2025-06-13)
───────────────────
* Preserve the **original** title supplied in the input list.
* Add a **Matched Title** column that records the canonical title returned by
  Open Library (if any). This avoids overwriting the user-provided text while
  still surfacing the authoritative name discovered via the API.

Author: Brendan Arbuckle
Date:2025-06-13
"""

from __future__ import annotations

import csv
import time
import urllib.parse
import requests
from typing import Tuple, List, Dict, Optional

# ――― Configuration ――― #
INPUT_FILE: str = "dragonlance_titles.txt"
OUTPUT_FILE: str = "enriched_dragonlance_data.csv"
SEARCH_LIMIT: int = 10  # how many Open Library search hits to examine

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
    "warforge",
    "dragonarmy",
    "chronicles",
    "legends",
    "companions",
    "dragonlance",
    "qualinosti",
    "istar",
    "tarsis",
    "thorbardin",
}

# Core novelists – extend as needed
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

DL_AUTHORS_LC: set[str] = {a.lower() for a in DL_AUTHORS}

# ――― Helper functions ――― #

def fetch_search_results(title: str, limit: int = SEARCH_LIMIT) -> List[Dict]:
    """Return a list of Open Library search docs for the given title."""
    url = (
        "https://openlibrary.org/search.json?"
        f"title={urllib.parse.quote(title)}&limit={limit}"
    )
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    return resp.json().get("docs", [])


def looks_like_dragonlance(doc: Dict) -> Tuple[bool, bool]:
    """Determine if a search doc is probably a Dragonlance work."""
    # ---------- author test ----------
    raw_authors = [a.lower() for a in doc.get("author_name", [])]
    has_author_hit = any(
        known in cand  # substring match handles "Margaret Weis & Tracy Hickman"
        for cand in raw_authors
        for known in DL_AUTHORS_LC
    )

    # ---------- keyword / subject / series test ----------
    subjects = [s.lower() for s in doc.get("subject", [])]
    series = [s.lower() for s in doc.get("series", [])]
    text_blobs = " ".join(subjects + series)
    has_keyword_hit = any(kw in text_blobs for kw in DL_KEYWORDS)

    return has_author_hit, has_keyword_hit


def pick_dragonlance_match(docs: List[Dict]) -> Optional[Dict]:
    """Pick the best candidate search result for Dragonlance metadata."""
    author_hits: List[Dict] = []
    keyword_hits: List[Dict] = []

    for d in docs:
        has_author, has_kw = looks_like_dragonlance(d)
        if has_author:
            author_hits.append(d)
        elif has_kw:
            keyword_hits.append(d)

    if author_hits:
        return author_hits[0]
    if keyword_hits:
        return keyword_hits[0]
    return docs[0] if docs else None


# ――― Main CLI ――― #

def main() -> None:
    print("🔍 Enriching Dragonlance titles with Open Library metadata …")

    # Read requested titles
    with open(INPUT_FILE, encoding="utf‑8") as f:
        titles: List[str] = [t.strip() for t in f if t.strip()]

    # Prepare CSV
    with open(OUTPUT_FILE, "w", newline="", encoding="utf‑8") as csvfile:
        fieldnames = [
            "Title",  # user‑supplied string
            "Matched Title",    # authoritative title from Open Library
            "Author(s)",
            "Series",
            "Publisher",
            "Publish Year",
            "ISBN",
            "OL Work Key",
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        # Process each title
        for title in titles:
            print(f" → {title}")
            try:
                docs = fetch_search_results(title)
                doc = pick_dragonlance_match(docs)

                if doc:
                    writer.writerow(
                        {
                            "Title": title,
                            "Matched Title": doc.get("title", ""),
                            "Author(s)": ", ".join(doc.get("author_name", [])),
                            "Series": doc.get("series", [""])[0] if "series" in doc else "",
                            "Publisher": ", ".join(doc.get("publisher", [])) if "publisher" in doc else "",
                            "Publish Year": doc.get("first_publish_year", ""),
                            "ISBN": ", ".join(doc.get("isbn", [])) if "isbn" in doc else "",
                            "OL Work Key": f"https://openlibrary.org{doc.get('key', '')}",
                        }
                    )
                else:
                    writer.writerow(
                        {
                            "Title": title,
                            "Matched Title": "Not Found",
                            "Author(s)": "Not Found",
                            "Series": "",
                            "Publisher": "Not Found",
                            "Publish Year": "Not Found",
                            "ISBN": "Not Found",
                            "OL Work Key": "Not Found",
                        }
                    )
            except Exception as exc:
                print(f"   ⚠️  Error: {exc}")
                writer.writerow(
                    {
                        "Title": title,
                        "Matched Title": "Error",
                        "Author(s)": "Error",
                        "Series": "",
                        "Publisher": "Error",
                        "Publish Year": "Error",
                        "ISBN": "Error",
                        "OL Work Key": "Error",
                    }
                )
            time.sleep(1)  # rate‑limit: be polite to the API

    print(f"\n✅ Done! Saved → {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
