"""Detect the language of papers.

Use fasttext language model to detect language. Process papers in parallel with
multiprocessing. Each child queries the database for a set of of papers,
loads them into memory and runs the language model on them.

The `Manager` is used to shared state between child processes.
This is necessary because we cannot have multiple processes writing to the 
database simultaneously. So, instead of each child writing to the database, 
they return the processed data to a queue. The main process
reads from the queue and writes to the items from the queue to the database.
"""


import fasttext
import os
import sqlite3 as sqlite
import time 
import pandas as pd
import argparse
from helpers.functions import print_elapsed_time, analyze_db
from helpers.variables import db_file, insert_questionmark_doctypes, keep_doctypes
from multiprocessing import Pool, cpu_count, Process, Queue
from queue import Empty

def get_project_root():
    """Return the path to the project root directory."""
    return os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', '..'))

# Set up the model path in the "data" directory
data_dir = os.path.join(get_project_root(), 'data')
model_path = os.path.join(data_dir, 'lid.176.ftz')

# Create the data directory if it doesn't exist
os.makedirs(data_dir, exist_ok=True)

print(f"Model path: {model_path}")

# Download the model if it doesn't exist
if not os.path.exists(model_path):
    print("Downloading FastText language identification model...")
    os.system(f'wget https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.ftz -O {model_path}')

# Load the FastText model
model = fasttext.load_model(model_path)

def detect_language(text):
    """
    Detect the language of the given text using FastText model.
    
    :param text: Input text
    :return: Tuple of (detected language code, confidence score)
    """
    if not text:
        return None, None
    predictions = model.predict(text, k=1)  # Get top 1 prediction
    lang = predictions[0][0].replace('__label__', '')  # Extract language code
    score = predictions[1][0]  # Extract confidence score
    return lang, score

def process_batch(papers):
    results = []
    for paper_id, title in papers:
        try:
            lang, score = detect_language(title)
            results.append((paper_id, lang, score))
        except Exception as e:
            print(f"Error processing paper {paper_id}: {e}")
    return results

def db_worker(queue, db_file):
    "Write from a queue into the database"
    con = sqlite.connect(database=db_file, isolation_level=None)
    cur = con.cursor()
    while True:
        try:
            results = queue.get(timeout=60)  # Wait for up to 60 seconds for new data
            if results == "DONE":
                break
            cur.executemany("INSERT INTO paper_language (PaperId, language, score) VALUES (?, ?, ?)", results)
            con.commit()
        except Empty:
            continue
    con.close()

def process_chunk(paper_ids):
    """Read titles for a chunk of papers from the database and pass them to `process_batch`.

    Args:
        paper_ids: List of PaperIds to process.

    Returns:
        results of `process_batch`

    """
    con = sqlite.connect(database=db_file, isolation_level=None)
    cur = con.cursor()
    placeholders = ','.join('?' for _ in paper_ids)
    cur.execute(f"""
        SELECT PaperId, OriginalTitle
        FROM Papers
        WHERE PaperId IN ({placeholders})
    """, paper_ids)
    papers = cur.fetchall()
    con.close()
    return process_batch(papers)

def parse_args():
    parser = argparse.ArgumentParser(description="Detect language of paper titles")
    parser.add_argument("--test", action="store_true", help="Run in test mode with 10,000 papers")
    return parser.parse_args()

def main():
    args = parse_args()
    
    # Connect to MAG db
    con = sqlite.connect(database=db_file, isolation_level=None)
    cur = con.cursor()

    # Create paper_language table (drop if exists)
    cur.execute("DROP TABLE IF EXISTS paper_language")
    cur.execute("""
        CREATE TABLE paper_language (
            PaperId INTEGER PRIMARY KEY,
            language TEXT,
            score REAL
        )
    """)

    # Get total number of papers
    if args.test:
        total_papers = 10000
    else:
        cur.execute("SELECT COUNT(*) FROM Papers")
        total_papers = cur.fetchone()[0]

    # Determine the number of processes to use
    num_processes = cpu_count()

    # Load PaperIds
    if args.test:
        cur.execute("SELECT PaperId FROM Papers LIMIT 10000")
    else:
        cur.execute("SELECT PaperId FROM Papers")
    all_paper_ids = [row[0] for row in cur.fetchall()]

    # Calculate chunk size and prepare chunks
    total_ids = len(all_paper_ids)
    chunk_size = -(-total_ids // num_processes)  # Ceiling division
    chunks = [all_paper_ids[i:i + chunk_size] for i in range(0, total_ids, chunk_size)]

    start_time = time.time()

    # Create a queue for database operations
    db_queue = Queue()

    # Start the database worker process
    db_process = Process(target=db_worker, args=(db_queue, db_file))
    db_process.start()

    with Pool(processes=num_processes) as pool:
        # Process papers in parallel
        results = pool.imap_unordered(process_chunk, chunks)

        # Put results in the queue and track progress
        processed_papers = 0
        for batch_result in results:
            db_queue.put(batch_result)
            processed_papers += len(batch_result)
            progress = processed_papers / total_papers * 100
            elapsed_time = time.time() - start_time
            print(f"\rProgress: {progress:.2f}% ({processed_papers}/{total_papers}) - "
                  f"Elapsed time: {elapsed_time:.2f} seconds", end="", flush=True)

    print()  # New line after progress

    # Signal the database worker to finish
    db_queue.put("DONE")
    db_process.join()

    print("Language detection completed.")
    print_elapsed_time(start_time)

    # Analyze the results
    cur.execute("SELECT COUNT(*) FROM paper_language")
    processed_papers = cur.fetchone()[0]
    print(f"Processed papers: {processed_papers}")

    cur.execute("SELECT language, COUNT(*), AVG(score) FROM paper_language GROUP BY language ORDER BY COUNT(*) DESC LIMIT 10")
    top_languages = cur.fetchall()
    print("Top 10 detected languages:")
    for lang, count, avg_score in top_languages:
        print(f"{lang}: {count} (Avg. score: {avg_score:.4f})")

    # Perform checks only when not in test mode
    if not args.test:
        print("Performing integrity checks...")
        
        # Check for uniqueness of PaperId in paper_language table
        cur.execute("SELECT COUNT(*) FROM paper_language")
        total_rows = cur.fetchone()[0]
        cur.execute("SELECT COUNT(DISTINCT PaperId) FROM paper_language")
        unique_papers = cur.fetchone()[0]
        if total_rows == unique_papers:
            print("✓ All PaperIds in paper_language table are unique.")
        else:
            print(f"⚠ Warning: Found {total_rows - unique_papers} duplicate PaperIds in paper_language table.")

        # Check if each PaperId in paper_language exists in Papers table
        cur.execute("""
            SELECT COUNT(*) 
            FROM paper_language pl
            LEFT JOIN Papers p ON pl.PaperId = p.PaperId
            WHERE p.PaperId IS NULL
        """)
        missing_papers = cur.fetchone()[0]
        if missing_papers == 0:
            print("✓ All PaperIds in paper_language table exist in Papers table.")
        else:
            msg= f"Found {missing_papers} PaperIds in paper_language that don't exist in Papers table."
            raise RuntimeError(msg)

    # Create an index on PaperId for the paper_language table
    print("Creating index on PaperId for paper_language table...")
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_paper_language_paperid ON paper_language (PaperId)")
    print("Index created successfully.")

    con.close()

if __name__ == "__main__":
    main()
