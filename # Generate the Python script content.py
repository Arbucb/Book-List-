# Generate the Python script content

import csv
import requests
import time
import urllib.parse

INPUT_FILE = "dragonlance_titles.txt"
OUTPUT_FILE = "enriched_dragonlance_data.csv"

# Read titles from input file
with open(INPUT_FILE, "r", encoding="utf-8") as file:
    titles = [line.strip() for line in file if line.strip()]

# Prepare CSV output
with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as csvfile:
    fieldnames = ["Title", "Author(s)", "Publisher", "Publish Year", "ISBN", "OL Work Key"]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for title in titles:
        print(f"Searching Open Library for: {title}")
        try:
            url = f"https://openlibrary.org/search.json?title={urllib.parse.quote(title)}&limit=1"
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                if data["docs"]:
                    doc = data["docs"][0]
                    writer.writerow({
                        "Title": doc.get("title", ""),
                        "Author(s)": ", ".join(doc.get("author_name", [])),
                        "Publisher": ", ".join(doc.get("publisher", [])) if "publisher" in doc else "",
                        "Publish Year": doc.get("first_publish_year", ""),
                        "ISBN": ", ".join(doc.get("isbn", [])) if "isbn" in doc else "",
                        "OL Work Key": f"https://openlibrary.org{doc.get('key', '')}"
                    })
                else:
                    writer.writerow({
                        "Title": title,
                        "Author(s)": "Not Found",
                        "Publisher": "Not Found",
                        "Publish Year": "Not Found",
                        "ISBN": "Not Found",
                        "OL Work Key": "Not Found"
                    })
            else:
                print(f"HTTP error {response.status_code} for {title}")
                writer.writerow({
                    "Title": title,
                    "Author(s)": "HTTP Error",
                    "Publisher": "HTTP Error",
                    "Publish Year": "HTTP Error",
                    "ISBN": "HTTP Error",
                    "OL Work Key": "HTTP Error"
                })
        except Exception as e:
            print(f"Error fetching data for {title}: {e}")
            writer.writerow({
                "Title": title,
                "Author(s)": "Error",
                "Publisher": "Error",
                "Publish Year": "Error",
                "ISBN": "Error",
                "OL Work Key": "Error"
            })
        time.sleep(1)  # Be kind to Open Library's servers
