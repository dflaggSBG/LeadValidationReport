#!/usr/bin/env python3
"""
Reset the DuckDB database by deleting the file and starting fresh
"""
import os
import shutil
from pathlib import Path

def main():
    project_root = Path(__file__).parent
    data_dir = project_root / "data"
    
    # Database file path
    db_file = data_dir / "leads.duckdb"
    
    print("🗑️ Resetting Lead Validation Database...")
    
    # Remove database file if it exists
    if db_file.exists():
        try:
            os.remove(db_file)
            print(f"✅ Removed existing database: {db_file}")
        except Exception as e:
            print(f"❌ Failed to remove database: {e}")
            return False
    else:
        print("ℹ️ No existing database found")
    
    # Recreate data directory if needed
    data_dir.mkdir(exist_ok=True)
    
    print("🎯 Database reset complete! Ready for fresh ETL run.")
    return True

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
