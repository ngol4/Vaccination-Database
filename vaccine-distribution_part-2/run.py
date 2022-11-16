import os

os.system('rm -f vaccination_db.db3')
os.system('sqlite3 vaccination_db.db3 < ./create_tables.sql')
import populate_data