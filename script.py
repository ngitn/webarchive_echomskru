import requests
from bs4 import BeautifulSoup
import sqlite3
import time
import sys
import argparse
import os

def setup_database():
    conn = sqlite3.connect('progress.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS pages (
            url TEXT PRIMARY KEY,
            processed INTEGER DEFAULT 0
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS files (
            url TEXT PRIMARY KEY,
            status TEXT,
            reason TEXT
        )
    ''')
    conn.commit()
    return conn

def log(message):
    print(f"{message}")

def validate_and_construct_url(base_url, link):
    from urllib.parse import urljoin
    return urljoin(base_url, link) if not link.startswith(('http:', 'https:')) else link

def fetch_page(url, sleep_time, retries=3, wait=190):
    for attempt in range(retries):
        try:
            time.sleep(sleep_time)
            response = requests.get(url)
            if response.status_code in range(400, 600):
                log(f"Skipping {url} due to error {response.status_code}")
                return None, response.status_code
            response.raise_for_status()
            return response.text, None
        except requests.ConnectionError:
            log(f"Connection error at {url}, retrying in {wait} seconds")
            time.sleep(wait)
        except requests.RequestException as e:
            log(f"Error requesting {url}: {e}")
            time.sleep(wait)
    log(f"Failed to fetch {url} after {retries} attempts.")
    return None, None

def process_subpage(url, content, page_folder):
    filename = url.split('/')[-2] + ".html" if url.endswith('/') else url.split('/')[-1]
    path = os.path.join(page_folder, filename)
    with open(path, "w") as file:
        file.write(content)
    log(f"Saved: {filename}")
    return filename

def process_page(base_url, url, conn, page_number, sleep_time, retry_skipped, pause_time):
    cursor = conn.cursor()
    folder_name = f"saved_pages/{page_number}"
    os.makedirs(folder_name, exist_ok=True)
    
    main_page_content, _ = fetch_page(url, sleep_time)
    if main_page_content:
        soup = BeautifulSoup(main_page_content, 'html.parser')
        for link in soup.select('.txt a.dark'):
            subpage_url = validate_and_construct_url(base_url, link.get('href'))
            existing_entry = cursor.execute("SELECT status FROM files WHERE url = ?", (subpage_url,)).fetchone()
            
            if existing_entry and not retry_skipped and existing_entry[0] == 'skipped':
                continue

            subpage_content, subpage_error_code = fetch_page(subpage_url, sleep_time)
            if subpage_content is None:
                cursor.execute("INSERT INTO files (url, status, reason) VALUES (?, 'skipped', ?)", (subpage_url, f"HTTP error {subpage_error_code}"))
            else:
                saved_filename = process_subpage(subpage_url, subpage_content, folder_name)
                cursor.execute("INSERT INTO files (url, status) VALUES (?, 'done')", (subpage_url,))
            conn.commit()
            time.sleep(pause_time)  # Apply pause even between subpages

    cursor.execute("INSERT INTO pages (url, processed) VALUES (?, 1)", (url,))
    conn.commit()

def crawl(base_url, sleep_time, pages_before_pause, pause_time, retry_skipped):
    conn = setup_database()
    cursor = conn.cursor()

    resume_url = cursor.execute("SELECT url FROM pages WHERE processed = 0").fetchone()
    queue = [(base_url, 1)] if not resume_url else [(resume_url[0], int(resume_url[0].rsplit('/', 2)[-2]))]

    page_counter = 0
    while queue:
        current_url, page_number = queue.pop(0)
        log(f"Processing: {current_url}")
        process_page(base_url, current_url, conn, page_number, sleep_time, retry_skipped, pause_time)

        if page_counter >= pages_before_pause:
            log(f"Pausing for {pause_time} seconds...")
            time.sleep(pause_time)
            page_counter = 0
        page_counter += 1

    conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Web crawler for specific content extraction.")
    parser.add_argument('-u', '--url', help="Base URL to start crawling from.")
    parser.add_argument('-s', '--speed', type=int, default=0, help="Speed of page loading and processing (0-10 seconds).")
    parser.add_argument('-p', '--pages', type=int, default=10, help="Number of pages to load before pausing.")
    parser.add_argument('-t', '--pause', type=int, default=60, help="Pause duration in seconds.")
    parser.add_argument('-r', '--retry', action='store_true', help="Retry downloading pages marked as 'skipped'.")

    args = parser.parse_args()

    if not args.url:
        print("No URL provided. Usage: python scriptname.py -u [URL] -s [SPEED] -p [PAGES] -t [PAUSE] -r")
        sys.exit(1)

    if args.speed < 0 or args.speed > 10:
        print("Speed must be between 0 and 10.")
        sys.exit(1)

    if args.pages < 0:
        print("Number of pages before pause must be a positive integer.")
        sys.exit(1)

    if args.pause < 0:
        print("Pause time must be a positive integer.")
        sys.exit(1)

    crawl(args.url, args.speed, args.pages, args.pause, args.retry)
