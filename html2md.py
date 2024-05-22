import os
import sqlite3
from pyquery import PyQuery as pq
import argparse
import logging
from termcolor import colored

# Set up logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

def initialize_db():
    conn = sqlite3.connect('htmls2txt.db')
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS files (
            path TEXT PRIMARY KEY,
            status TEXT,
            length INTEGER,
            reason TEXT
        )
    ''')
    conn.commit()
    conn.close()
    logging.info('Database initialized and table created.')

def log_file(path, status, length=0, reason=""):
    conn = sqlite3.connect('htmls2txt.db')
    c = conn.cursor()
    c.execute('INSERT OR REPLACE INTO files (path, status, length, reason) VALUES (?, ?, ?, ?)',
              (path, status, length, reason))
    conn.commit()
    conn.close()
    if status == 'done':
        logging.info(f'File logged: {path} - Status: {status}, Length: {length}')
    else:
        logging.warning(f'File logged: {path} - Status: {status}, Length: {length}, Reason: {reason}')

def should_process_file(path):
    conn = sqlite3.connect('htmls2txt.db')
    c = conn.cursor()
    c.execute('SELECT status FROM files WHERE path=?', (path,))
    result = c.fetchone()
    conn.close()
    return result is None or (args.retry and result[0] == 'skipped')

def extract_text(html_content):
    doc = pq(html_content)
    text = doc('.mmplayer').text()
    if not text:
        text = doc('.conthead.discuss').text()
    return text

def process_folder(folder_path):
    markdown_output_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "scrap_texts.md")
    for subdir, dirs, files in os.walk(folder_path):
        for file in files:
            file_path = os.path.join(subdir, file)
            if should_process_file(file_path):
                try:
                    with open(file_path, 'r') as f:
                        text = extract_text(f.read())
                        if text:
                            with open(markdown_output_path, 'a') as md:
                                md.write(text + "\n\n\n\n---\n\n\n\n\n\n")
                            log_file(file_path, 'done', len(text))
                            logging.info(colored(f'Processed and appended to markdown: {file_path}', 'green'))
                        else:
                            log_file(file_path, 'skipped', reason='No content in .mmplayer or .conthead.discuss')
                            logging.warning(colored(f'Skipped (No content): {file_path}', 'red'))
                except Exception as e:
                    log_file(file_path, 'skipped', reason=str(e))
                    logging.error(colored(f'Error processing {file_path}: {e}', 'red'))

parser = argparse.ArgumentParser()
parser.add_argument('--retry', action='store_true', help="Retry processing skipped files")
args = parser.parse_args()

if __name__ == '__main__':
    initialize_db()
    process_folder('saved_pages')
