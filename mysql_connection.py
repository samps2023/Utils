import connectorx as cx
import sqlalchemy
import pandas as pd
import os 
from jinja2 import Template
from datetime import datetime
from urllib.parse import quote_plus
import pytz
import polars as pl

def is_numeric(value):
    """Check if a value is numeric (including negative numbers and floats)."""
    try:
        float(value)  # Attempt to convert to float
        return True
    except ValueError:
        return False
    
class MySQL:
    def __init__(self, server, username, password, port_num):
        self.server = server
        self.username = username
        self.password = password
        self.port_num = port_num

    @classmethod
    def from_env(cls, conn_name: str):
        prefix = conn_name.upper()
        return cls(
            server=os.getenv(f"{prefix}_SERVER"),
            username=os.getenv(f"{prefix}_USERNAME"),
            password=os.getenv(f"{prefix}_PASSWORD"),
            port_num=int(os.getenv(f"{prefix}_PORT")),
        )
    
    def database_url(self):
        enconded_username = quote_plus(self.username)
        encoded_password = quote_plus(self.password)
        mysql_url = f"mysql://{enconded_username}:{encoded_password}@{self.server}:{self.port_num}"
        return mysql_url
    
    def database_engine(self):
        encoded_username = quote_plus(self.username)
        encoded_password = quote_plus(self.password)
        return sqlalchemy.create_engine(
        url=f"mysql+mysqlconnector://{encoded_username}:{encoded_password}@{self.server}:{self.port_num}"
        )
    
    def sql_statement(self,file_name: str, variables: dict = {}):
        with open(
                f"{file_name}", "r", encoding="utf-8"
        ) as file:
            sql_template = Template(file.read())

            formatted_variables = {
                  k: f"'{v}'" if not is_numeric(v) else v 
                  for k,v in variables.items()
            }
            rendered_sql = sql_template.render(**formatted_variables)
        return rendered_sql

    def read_sql_file(self,file_name: str, variables: dict = {}):
        
        # Split the SQL script into individual statements
        sql_statements = self.sql_statement(file_name, variables).split(';')
        
        # Remove any empty statements
        sql_statements = [stmt.strip() for stmt in sql_statements if stmt.strip()]
        
        with self.database_engine().connect() as conn:
            for statement in sql_statements:
                if statement.lower().startswith(('select', 'with')):
                    # Execute the SELECT statement and store the result
                    result_df = pd.read_sql_query(sql=sqlalchemy.text(statement), con=conn)
                else:
                    # Execute other statements (e.g., USE, INSERT, UPDATE)
                    conn.execute(sqlalchemy.text(statement))
        return result_df

    def read_query(self,query:str,variables: dict = {}):
        sql_template = Template(query)
        rendered_sql = sql_template.render(**variables)

        with self.database_engine().connect() as conn:
            df = pd.read_sql_query(sql=sqlalchemy.text(rendered_sql), con=conn)
            return df
    
    def connx(self,file_name: str,df_type: str ='pl', variables: dict = {}):
        if df_type =='pd':
            df = cx.read_sql(self.database_url(), self.sql_statement(file_name,variables))
        else:
            df = pl.read_database_uri(self.sql_statement(file_name,variables), uri =self.database_url(),engine='connectorx')
        return df

    def get_column_dtypes(self, df: pd.DataFrame, custom_dtype: dict):
        dtypedict = {}
        for i, j in zip(df.columns, df.dtypes):
            if "object" in str(j):
                dtypedict.update({i: sqlalchemy.types.NVARCHAR(length=255)})

            if "datetime" in str(j):
                dtypedict.update({i: sqlalchemy.types.DateTime()})

            if "float" in str(j):
                dtypedict.update({i: sqlalchemy.types.Numeric(precision=20, scale=6)})

            if "int" in str(j):
                dtypedict.update({i: sqlalchemy.types.INT()})

        if len(custom_dtype) > 0:
            for key, value in custom_dtype.items():
                dtypedict.update({key: value})
        return dtypedict

    def generate_updated_time_column(self, df: pd.DataFrame):
        utc_timezone = pytz.timezone("UTC")
        current_time_utc = datetime.now(utc_timezone)
        df["updated_time_utc"] = current_time_utc
        return df

    def load_data(
            self,
            df: pd.DataFrame,
            table_name: str,
            schema_name: str,
            method='replace', #replace/append
            generate_updated_time_column: bool = True,
            custom_dtype: dict = {},
            ): 
        if generate_updated_time_column:
            df = self.generate_updated_time_column(df)

        col_type = self.get_column_dtypes(df,custom_dtype)

        with self.database_engine().connect() as conn:
            df.to_sql(name=table_name,con=conn,schema=schema_name, if_exists=method, index=False, dtype=col_type, chunksize=30000)
    


