import sqlite3
import pandas as pd
conn = sqlite3.connect('olap_retail_warehouse.db')
cursor = conn.cursor()