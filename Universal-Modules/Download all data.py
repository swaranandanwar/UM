# Modules for Metric Calculations
import sys
path = r"C:\Users\adamszeq\Desktop\Clones\Universal-Modules"
sys.path.append(path)
import sqlalchemy as sa
from urllib.parse import quote
import urllib
import pandas as pd
from sqlalchemy.sql import text
import time
from common import cfg
import pyarrow as pa
import pyarrow.parquet as pq

class sqlConnection():

    def __init__(self, host_sql, path, query):
        self.host_sql = host_sql
        self.path = path
        self.query = query

    def create_conexion(self):
        server = cfg[self.host_sql]['host']
        database = cfg[self.host_sql]['db']
        username= cfg[self.host_sql]['user']
        password= cfg[self.host_sql]['passwd']
        conn = urllib.parse.quote_plus(
    'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + quote(username) + ';PWD=' + password
    )
        engine = sa.create_engine('mssql+pyodbc:///?odbc_connect={}'.format(conn))
        return engine

    def readSQL(self, query):
        engine = self.create_conexion()
        df = pd.read_sql(query, engine)
        return df

    def writeSQL(self, query):
        engine = self.create_conexion()
        with engine.connect() as con:
            con.execute(text(query))

    def writeToParquet(self, query, path):
        start_time = time.time()
        engine = self.create_conexion()
        df = self.readSQL(query)
        total_rows = len(df)
        print('Total Rows are : ', total_rows)
        processed_rows = 0
        batch_size = 1000
        for i in range(0, total_rows, batch_size):
            df_batch = df.iloc[i:i+batch_size]
            table = pa.Table.from_pandas(df_batch)
            pq.write_table(table, path)
            processed_rows += len(df_batch)
            percentage_complete = (processed_rows / total_rows) * 100
            print(f"{percentage_complete:.2f}% complete")
        end_time = time.time()
        print(f"Time taken to execute {query} : {end_time - start_time}")

    def readFromParquet(self, path):
        table = pq.read_table(path)
        return table.to_pandas()

if __name__ == "__main__":
    host = 'SQL2'
    queries = [("Home Renewals Monitor", "SELECT * FROM [OP].[OP].[RenewalHomeMonitor]", r'C:\Users\adamszeq\Desktop\Clones\Universal-Modules\Data\HomeRenewalsMonitor.parquet'),
               ("Home NB Monitor", "SELECT * FROM [OP].[OP].[NBHomeMonitor]", r'C:\Users\adamszeq\Desktop\Clones\Universal-Modules\Data\homeNBMonitor.parquet'),
               ("Motor Renewals Monitor", "SELECT * FROM [BI].[BI].[MotorRenewals]", r'C:\Users\adamszeq\Desktop\Clones\Universal-Modules\Data\motorRenewalsMonitor.parquet'),
               ("Membership Renewals Monitor", "SELECT * FROM [BI].[BI].[MembershipRenewals]", r'C:\Users\adamszeq\Desktop\Clones\Universal-Modules\Data\membershipRenewals.parquet')
              ]
    object = sqlConnection(host, "", "")
    for query in queries:
        print(f"Running {query[0]}...")
        object.writeToParquet(query[1], query[2])
        print(f"{query[0]} complete!")