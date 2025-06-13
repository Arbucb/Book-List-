import csv
import requests
import time

INPUT_FILE = "enriched_dragonlance_data.csv"
OUTPUT_FILE = "final_dragonlance_data.csv"

def query_google_books(title, isbn=None):
    try:
        query = f"intitle:{title}"
        if isbn:
            query += f"+isbn:{isbn}"
        url = f"https://www.googleapis.com/books/v1/volumes?q={query}"
        response = requests.get(url)
        if response.status_code == 200:
            items = response.json().get("items")
            if items:
                volume_info = items[0].get("volumeInfo", {})
                return {
                    "Title": volume_info.get("title", ""),
                    "Author(s)": ", ".join(volume_info.get("authors", [])),
                    "Publisher": volume_info.get("publisher", ""),
                    "Publish Year": volume_info.get("publishedDate", ""),
                    "Page Count": volume_info.get("pageCount", ""),
                    "Categories": ", ".join(volume_info.get("categories", [])) if "categories" in volume_info else "",
                    "Description": volume_info.get("description", ""),
                    "Cover Image": volume_info.get("imageLinks", {}).get("thumbnail", "")
                }
    except Exception as e:
        print(f"Error querying Google Books for '{title}': {e}")
    return {}

with open(INPUT_FILE, "r", encoding="utf-8") as infile, open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as outfile:
    reader = csv.DictReader(infile)
    fieldnames = reader.fieldnames + ["Page Count", "Categories", "Description", "Cover Image"]
    writer = csv.DictWriter(outfile, fieldnames=fieldnames)
    writer.writeheader()

    for row in reader:
        title = row.get("Title")
        isbn = row.get("ISBN").split(",")[0] if row.get("ISBN") else None

        print(f"Looking up: {title}")
        result = query_google_books(title, isbn)

        # Fill in missing fields only
        for key in ["Author(s)", "Publisher", "Publish Year"]:
            if not row.get(key) or row.get(key) in ["Not Found", "Error", ""]:
                row[key] = result.get(key, row.get(key))

        # Add new fields from Google
        row["Page Count"] = result.get("Page Count", "")
        row["Categories"] = result.get("Categories", "")
        row["Description"] = result.get("Description", "")
        row["Cover Image"] = result.get("Cover Image", "")

        writer.writerow(row)
        time.sleep(1)  # Respect rate limits