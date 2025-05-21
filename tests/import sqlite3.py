import sqlite3
import shutil

# Path to the original database
original_db = 'tms_data.db'
# Path to the new database
new_db = 'test_data.db'

# Copy the original database to the new location
shutil.copyfile(original_db, new_db)

print(f"Database copied from {original_db} to {new_db} successfully.")
