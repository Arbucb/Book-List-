"""
Fetch Dragonlance book metadata from Googles API.

Given the output from Google API Lookup, the script writes a CSV containing only the
matching Dragonlance works even when identically titled, non-Dragonlance books
exist.

Author: Brendan Arbuckle
Date: 2025-06-13
"""

import csv
import requests
import time
import re

INPUT_FILE = "final_dragonlance_data.csv" 
OUTPUT_FILE  = "refined_final_dragonlance_data.csv"

# Anything that looks like “unknown” counts as missing
UNKNOWN_RE = re.compile(r"^\s*$|^\s*\[?unknown\]?\s*$", re.I)

def query_google_books(title, isbn=None):
    """
    Return basic metadata from Google Books for a Dragonlance title.
    Only the first result is used.
    """
    try:
        # Add +Dragonlance to keep the match on brand
        query = f'intitle:"{title}" +Dragonlance'
        if isbn:
            query += f"+isbn:{isbn.split()[0]}"
        url = f"https://www.googleapis.com/books/v1/volumes?q={query}&maxResults=1"
        r   = requests.get(url, timeout=10)
        if r.status_code == 200:
            items = r.json().get("items", [])
            if items:
                info = items[0].get("volumeInfo", {})
                return {
                    "Title":        info.get("title", ""),
                    "Author(s)":    ", ".join(info.get("authors", [])),
                    "Publisher":    info.get("publisher", ""),
                    "Publish Year": info.get("publishedDate", ""),
                    "Page Count":   info.get("pageCount", ""),
                    "Categories":   ", ".join(info.get("categories", [])) if "categories" in info else "",
                    "Description":  info.get("description", ""),
                    "Cover Image":  info.get("imageLinks", {}).get("thumbnail", ""),
                }
    except Exception as exc:
        print(f"⚠️  Error querying Google Books for “{title}”: {exc}")
    return {}

with open(INPUT_FILE, "r", encoding="utf-8") as infile, \
     open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as outfile:

    reader = csv.DictReader(infile)
    # Ensure the extra columns exist exactly once
    extra_cols = ["Page Count", "Categories", "Description", "Cover Image"]
    fieldnames = reader.fieldnames + [c for c in extra_cols if c not in reader.fieldnames]
    writer = csv.DictWriter(outfile, fieldnames=fieldnames)
    writer.writeheader()

    for row in reader:
        title = row.get("Title", "").strip()
        isbn  = (row.get("ISBN") or "").split(",")[0].strip() or None

        print(f"🔍 Looking up: {title}")
        data = query_google_books(title, isbn)

        # Update Author(s) / Publisher / Publish Year if missing or “unknown”
        for key in ["Author(s)", "Publisher", "Publish Year"]:
            current = (row.get(key) or "").strip()
            if UNKNOWN_RE.match(current):
                row[key] = data.get(key, current) or current

        # Always fill the extra Google-specific columns
        row["Page Count"] = data.get("Page Count", row.get("Page Count", ""))
        row["Categories"] = data.get("Categories", row.get("Categories", ""))
        row["Description"] = data.get("Description", row.get("Description", ""))
        row["Cover Image"] = data.get("Cover Image", row.get("Cover Image", ""))

        writer.writerow(row)
        time.sleep(1)   # Google Books free tier: be courteous