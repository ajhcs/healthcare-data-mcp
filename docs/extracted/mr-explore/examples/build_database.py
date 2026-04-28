"""
Build Database Script
----------------------
Processes CSV files from data_source/ and creates a normalized SQLite database.

Usage:
    python scripts/build_database.py

Output:
    dist/hospital_data.db
"""

import csv
import os
import sqlite3
import sys
from pathlib import Path

# Configuration
DATA_SOURCE_DIR = Path(__file__).parent.parent / "data_source"
OUTPUT_DIR = Path(__file__).parent.parent / "dist"
OUTPUT_DB = OUTPUT_DIR / "hospital_data.db"

# Schema
SCHEMA = """
-- Lookup Tables
CREATE TABLE IF NOT EXISTS descriptions (
    id INTEGER PRIMARY KEY,
    text TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS algorithms (
    id INTEGER PRIMARY KEY,
    text TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS payers (
    id INTEGER PRIMARY KEY,
    name TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS plans (
    id INTEGER PRIMARY KEY,
    name TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS methodologies (
    id INTEGER PRIMARY KEY,
    name TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS hospitals (
    id INTEGER PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    location TEXT,
    address TEXT
);

-- Main Charges Table (normalized)
CREATE TABLE IF NOT EXISTS charges (
    id INTEGER PRIMARY KEY,
    hospital_id INTEGER NOT NULL REFERENCES hospitals(id),
    description_id INTEGER REFERENCES descriptions(id),
    code1 TEXT,
    code1_type TEXT,
    code2 TEXT,
    code2_type TEXT,
    modifiers TEXT,
    setting TEXT,
    drug_unit TEXT,
    drug_type TEXT,
    gross_charge REAL,
    discounted_cash REAL,
    payer_id INTEGER REFERENCES payers(id),
    plan_id INTEGER REFERENCES plans(id),
    negotiated_dollar REAL,
    negotiated_percentage REAL,
    algorithm_id INTEGER REFERENCES algorithms(id),
    estimated_amount REAL,
    methodology_id INTEGER REFERENCES methodologies(id),
    min_charge REAL,
    max_charge REAL,
    notes TEXT,
    billing_class TEXT
);

-- FTS5 External Content Table for descriptions
CREATE VIRTUAL TABLE IF NOT EXISTS descriptions_fts USING fts5(
    text,
    content='descriptions',
    content_rowid='id'
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_charges_hospital ON charges(hospital_id);
CREATE INDEX IF NOT EXISTS idx_charges_description ON charges(description_id);
CREATE INDEX IF NOT EXISTS idx_charges_payer ON charges(payer_id);
CREATE INDEX IF NOT EXISTS idx_charges_code1 ON charges(code1);
"""


def parse_float(val: str) -> float | None:
    """Safely parse a float from a string."""
    if not val or val.strip() == "":
        return None
    try:
        return float(val.replace(",", ""))
    except ValueError:
        return None


def get_or_create_id(cursor: sqlite3.Cursor, table: str, column: str, value: str, cache: dict) -> int | None:
    """Get or create a lookup table entry, using a cache for performance."""
    if not value or value.strip() == "":
        return None
    
    if value in cache:
        return cache[value]
    
    cursor.execute(f"INSERT OR IGNORE INTO {table} ({column}) VALUES (?)", (value,))
    cursor.execute(f"SELECT id FROM {table} WHERE {column} = ?", (value,))
    row = cursor.fetchone()
    if row:
        cache[value] = row[0]
        return row[0]
    return None


def process_csv(filepath: Path, conn: sqlite3.Connection, caches: dict) -> int:
    """Process a single CSV file and insert into the database."""
    cursor = conn.cursor()
    row_count = 0
    
    # Extract hospital info from first two lines
    with open(filepath, "r", encoding="utf-8", errors="replace") as f:
        # Line 1: metadata header (skip)
        next(f)
        # Line 2: hospital info
        reader = csv.reader(f)
        hospital_row = next(reader)
        hospital_name = hospital_row[0] if len(hospital_row) > 0 else filepath.stem
        hospital_location = hospital_row[3] if len(hospital_row) > 3 else None
        hospital_address = hospital_row[4] if len(hospital_row) > 4 else None
        
        # Get or create hospital
        cursor.execute("INSERT OR IGNORE INTO hospitals (name, location, address) VALUES (?, ?, ?)",
                       (hospital_name, hospital_location, hospital_address))
        cursor.execute("SELECT id FROM hospitals WHERE name = ?", (hospital_name,))
        hospital_id = cursor.fetchone()[0]
        
        # Line 3: data headers
        headers = next(reader)
        
        # Process data rows
        batch = []
        for row in reader:
            if len(row) < len(headers):
                row.extend([""] * (len(headers) - len(row)))
            
            # Get lookup IDs
            desc_id = get_or_create_id(cursor, "descriptions", "text", row[0], caches["descriptions"])
            payer_id = get_or_create_id(cursor, "payers", "name", row[11] if len(row) > 11 else "", caches["payers"])
            plan_id = get_or_create_id(cursor, "plans", "name", row[12] if len(row) > 12 else "", caches["plans"])
            algo_id = get_or_create_id(cursor, "algorithms", "text", row[15] if len(row) > 15 else "", caches["algorithms"])
            method_id = get_or_create_id(cursor, "methodologies", "name", row[17] if len(row) > 17 else "", caches["methodologies"])
            
            batch.append((
                hospital_id,
                desc_id,
                row[1] if len(row) > 1 else None,   # code1
                row[2] if len(row) > 2 else None,   # code1_type
                row[3] if len(row) > 3 else None,   # code2
                row[4] if len(row) > 4 else None,   # code2_type
                row[5] if len(row) > 5 else None,   # modifiers
                row[6] if len(row) > 6 else None,   # setting
                row[7] if len(row) > 7 else None,   # drug_unit
                row[8] if len(row) > 8 else None,   # drug_type
                parse_float(row[9]) if len(row) > 9 else None,   # gross_charge
                parse_float(row[10]) if len(row) > 10 else None, # discounted_cash
                payer_id,
                plan_id,
                parse_float(row[13]) if len(row) > 13 else None, # negotiated_dollar
                parse_float(row[14]) if len(row) > 14 else None, # negotiated_percentage
                algo_id,
                parse_float(row[16]) if len(row) > 16 else None, # estimated_amount
                method_id,
                parse_float(row[18]) if len(row) > 18 else None, # min_charge
                parse_float(row[19]) if len(row) > 19 else None, # max_charge
                row[20] if len(row) > 20 else None,              # notes
                row[21] if len(row) > 21 else None,              # billing_class
            ))
            
            row_count += 1
            
            # Batch insert every 10000 rows
            if len(batch) >= 10000:
                cursor.executemany("""
                    INSERT INTO charges (
                        hospital_id, description_id, code1, code1_type, code2, code2_type,
                        modifiers, setting, drug_unit, drug_type, gross_charge, discounted_cash,
                        payer_id, plan_id, negotiated_dollar, negotiated_percentage,
                        algorithm_id, estimated_amount, methodology_id, min_charge, max_charge,
                        notes, billing_class
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, batch)
                batch = []
        
        # Insert remaining batch
        if batch:
            cursor.executemany("""
                INSERT INTO charges (
                    hospital_id, description_id, code1, code1_type, code2, code2_type,
                    modifiers, setting, drug_unit, drug_type, gross_charge, discounted_cash,
                    payer_id, plan_id, negotiated_dollar, negotiated_percentage,
                    algorithm_id, estimated_amount, methodology_id, min_charge, max_charge,
                    notes, billing_class
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, batch)
    
    conn.commit()
    return row_count


def main():
    print("=" * 60)
    print("Hospital Data Build Pipeline")
    print("=" * 60)
    
    # Create output directory
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # Remove existing database
    if OUTPUT_DB.exists():
        OUTPUT_DB.unlink()
        print(f"Removed existing database: {OUTPUT_DB}")
    
    # Check for source files
    if not DATA_SOURCE_DIR.exists():
        print(f"ERROR: Data source directory not found: {DATA_SOURCE_DIR}")
        print("Please create this directory and place CSV files in it.")
        sys.exit(1)
    
    csv_files = list(DATA_SOURCE_DIR.glob("*.csv"))
    if not csv_files:
        print(f"ERROR: No CSV files found in {DATA_SOURCE_DIR}")
        sys.exit(1)
    
    print(f"Found {len(csv_files)} CSV files to process.")
    
    # Connect to database
    conn = sqlite3.connect(OUTPUT_DB)
    conn.execute("PRAGMA journal_mode = OFF")
    conn.execute("PRAGMA synchronous = OFF")
    conn.execute("PRAGMA cache_size = -100000")  # 100MB cache
    conn.execute("PRAGMA temp_store = MEMORY")
    
    # Create schema
    conn.executescript(SCHEMA)
    
    # Initialize caches for lookup tables
    caches = {
        "descriptions": {},
        "payers": {},
        "plans": {},
        "algorithms": {},
        "methodologies": {},
    }
    
    # Process each CSV file
    total_rows = 0
    for i, csv_file in enumerate(csv_files, 1):
        print(f"[{i}/{len(csv_files)}] Processing {csv_file.name}...")
        try:
            rows = process_csv(csv_file, conn, caches)
            total_rows += rows
            print(f"    -> {rows:,} rows imported")
        except Exception as e:
            print(f"    -> ERROR: {e}")
    
    print("-" * 60)
    print(f"Total rows imported: {total_rows:,}")
    
    # Rebuild FTS index
    print("Building FTS index...")
    conn.execute("INSERT INTO descriptions_fts(descriptions_fts) VALUES('rebuild')")
    conn.commit()
    
    # Optimize database
    print("Optimizing database...")
    conn.execute("ANALYZE")
    conn.execute("VACUUM")
    conn.close()
    
    # Report final size
    final_size = OUTPUT_DB.stat().st_size / (1024 * 1024)
    print(f"Database created: {OUTPUT_DB}")
    print(f"Final size: {final_size:.2f} MB")
    print("=" * 60)


if __name__ == "__main__":
    main()
