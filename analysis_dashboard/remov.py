#!/usr/bin/env python3
"""
Migration script to remove fused_snapshots table from analysis database.

Run this once to clean up the unused table from your existing database.
"""

import sqlite3
import os
from pathlib import Path

DB_PATH = "/app/data/analysis_cache.db"

def migrate():
    """Remove fused_snapshots table if it exists"""
    
    # Check if database exists
    if not os.path.exists(DB_PATH):
        print(f"✅ Database not found at {DB_PATH} - nothing to migrate")
        return
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Check if table exists
    cursor.execute("""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name='fused_snapshots'
    """)
    
    if cursor.fetchone():
        print(f"🗑️  Found fused_snapshots table - removing it...")
        
        # Get row count before deletion
        cursor.execute("SELECT COUNT(*) FROM fused_snapshots")
        count = cursor.fetchone()[0]
        
        # Drop the table
        cursor.execute("DROP TABLE fused_snapshots")
        
        # Drop the index if it exists
        cursor.execute("DROP INDEX IF EXISTS idx_fused_symbol")
        
        conn.commit()
        print(f"✅ Removed fused_snapshots table (had {count} rows)")
        print(f"✅ Removed idx_fused_symbol index")
        
        # Get database size
        cursor.execute("PRAGMA page_count")
        page_count = cursor.fetchone()[0]
        cursor.execute("PRAGMA page_size")
        page_size = cursor.fetchone()[0]
        db_size_mb = (page_count * page_size) / (1024 * 1024)
        
        print(f"📊 Database size: {db_size_mb:.2f} MB")
        print(f"💡 Consider running VACUUM to reclaim space")
        
        # Ask if user wants to vacuum
        print(f"\n🔧 Run VACUUM to reclaim disk space? (This may take a moment)")
        print(f"   This will compact the database and free up space from deleted data.")
        
    else:
        print(f"✅ fused_snapshots table not found - already clean")
    
    conn.close()


def vacuum_database():
    """Compact the database to reclaim space"""
    print(f"\n🔧 Vacuuming database...")
    
    conn = sqlite3.connect(DB_PATH)
    
    # Get size before
    cursor = conn.cursor()
    cursor.execute("PRAGMA page_count")
    page_count_before = cursor.fetchone()[0]
    cursor.execute("PRAGMA page_size")
    page_size = cursor.fetchone()[0]
    size_before_mb = (page_count_before * page_size) / (1024 * 1024)
    
    # Vacuum
    conn.execute("VACUUM")
    
    # Get size after
    cursor.execute("PRAGMA page_count")
    page_count_after = cursor.fetchone()[0]
    size_after_mb = (page_count_after * page_size) / (1024 * 1024)
    
    saved_mb = size_before_mb - size_after_mb
    
    print(f"✅ Vacuum complete")
    print(f"   Before: {size_before_mb:.2f} MB")
    print(f"   After:  {size_after_mb:.2f} MB")
    print(f"   Saved:  {saved_mb:.2f} MB")
    
    conn.close()


if __name__ == "__main__":
    print("=" * 60)
    print("  fused_snapshots Table Removal Migration")
    print("=" * 60)
    print()
    
    migrate()
    
    print()
    print("=" * 60)
    print("  Migration Complete")
    print("=" * 60)
    print()
    print("Next steps:")
    print("1. Replace fusion.py with the cleaned version")
    print("2. Replace main.py in analysis service")
    print("3. Restart the analysis-service container")
    print("4. Optional: Run vacuum_database() to reclaim disk space")