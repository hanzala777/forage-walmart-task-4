import pandas as pd
import sqlite3

spreadsheet_0 = 'data/shipping_data_0.csv'
spreadsheet_1 = 'data/shipping_data_1.csv'
spreadsheet_2 = 'data/shipping_data_2.csv'
database = 'shipping_database.db'

df0 = pd.read_csv(spreadsheet_0)
df1 = pd.read_csv(spreadsheet_1)
df2 = pd.read_csv(spreadsheet_2)

conn = sqlite3.connect(database)
cursor = conn.cursor()

cursor.execute('''
CREATE TABLE IF NOT EXISTS sd0 (
    product TEXT,
    on_time BOOLEAN,
    quantity INTEGER,
    origin_warehouse TEXT,
    destination_store TEXT,
    driver_identifier TEXT
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS sd2(
    shipment_id TEXT KEY,
    origin_warehouse TEXT,
    destination_store TEXT,
    driver_identifier TEXT
)
''')
cursor.execute('''
CREATE TABLE IF NOT EXISTS sd1 (
    shipment_id TEXT,
    product TEXT,
    on_time BOOLEAN,
    FOREIGN KEY (shipment_id) REFERENCES sd2(shipment_id)
)
''')

for _, row in df0.iterrows():
    cursor.execute('''
        INSERT INTO sd0 (product, on_time, quantity, origin_warehouse, destination_store, driver_identifier)
        VALUES (?, ?, ?, ?, ?, ?)
        ''', (row['product'], row['on_time'], row['product_quantity'], row['origin_warehouse'], row['destination_store'],
          row['driver_identifier']))


for _, row in df2.iterrows():
    shipment_id = row['shipment_identifier']

    location = df2[df2['shipment_identifier'] == shipment_id].iloc[0]
    origin_warehouse = location['origin_warehouse']
    destination_store = location['destination_store']
    driver_identifier = location['driver_identifier']

    cursor.execute('''
        INSERT INTO sd2 (shipment_id, origin_warehouse, destination_store, driver_identifier)
        VALUES(?, ?, ?, ?)
        ''', (shipment_id, origin_warehouse, destination_store, driver_identifier))

for _, row in df1.iterrows():
    shipment_id = row['shipment_identifier']
    product = row['product']
    on_time = row['on_time']

    location = df2[df2['shipment_identifier'] == shipment_id].iloc[0]
    origin_warehouse = location['origin_warehouse']
    destination_store = location['destination_store']
    driver_identifier = location['driver_identifier']

    cursor.execute('''
        INSERT INTO sd1 (shipment_id, product, on_time)
        VALUES (?, ?, ?)
    ''', (shipment_id, product, on_time))
    

conn.commit()
conn.close()

print('Data has been successfully inserted into the database.')
