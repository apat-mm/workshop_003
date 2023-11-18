"""
    all database related operations are performed in this file. 
"""

import json
import pandas as pd
import psycopg2


with open('./db_config.json', 'r') as archivo:
    datos = json.load(archivo)

try:
    conn = psycopg2.connect(
        host = 'localhost',
        user = datos['user'],
        password = datos['password'],
        database = datos['database']
    )
    print('¡Conexion exitosa!!')
    cursor = conn.cursor()
    
except Exception as ex:
    print(ex)


def create_table():
    sql = '''
    CREATE TABLE IF NOT EXISTS happiness
    (gdp float, 
    family float,
    healthy_life_expectancy float,
    freedom float,
    y_pred float
    );
       '''   
    cursor.execute(sql)
    conn.commit()
    print('¡Tabla creada!')


def insert_data(df):
    for index, row in df.iterrows():
        values = tuple(row.values)
        query = f"INSERT INTO happiness VALUES {values}"
        cursor.execute(query)
    conn.commit()
    print('¡Registros ingresados!')

if __name__ == "__main__":
    create_table()