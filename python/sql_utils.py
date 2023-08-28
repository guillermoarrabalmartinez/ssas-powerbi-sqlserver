import time
import os
import pandas as pd
import numpy as np
import sqlalchemy.schema
from sqlalchemy import create_engine, exc, types

class SQLDatabase:
    """Pandas SQL Alquemy"""
    def __init__(self,connection_string):
        """Different parameters to be passed in the instance creation"""
        self.engine = create_engine(connection_string)


    def connect_and_load_to_sqlserver(self, df, table_name: str, schema_name=None, if_exists='replace',column_types=None):
        """Function to connect and load a dataframe onto sqlserver sql:localhost
        :param df: dataframe to load to sql
        :param table_name: name of the table is going to be created
        :param schema_name: name of the schema in which the table will be allocated
        :param if_exists: in case you want to append change that parameter (replace/append)"""
        start_time = time.time()
        # Split the file name in case it has extension like: data.csv --> data
        table_name_simple = os.path.splitext(table_name)[0]
        # Create SQLAlchemy engine
        try:
            # Check if the schema exists
            #if not schema_exists(engine, schema_name):
            #    engine.execute(sqlalchemy.schema.CreateSchema(schema_name))
            #    raise ValueError(f"The schema '{schema_name}' does not exist.")
            df.to_sql(
                table_name_simple, 
                schema=schema_name, 
                con=self.engine, 
                if_exists=if_exists, 
                index=False,
                dtype=column_types)
            result_rows = f'Loaded {len(df)} rows INTO {table_name_simple} table.'
            result_time = "Table " + schema_name + "." + table_name_simple + " loaded in: {} seconds".format(time.time() - start_time)
            print(result_time)
            print(result_rows)
        except exc.SQLAlchemyError as e:
            print('An error occurred while loading the data into the SQL Server table.')
            print(f'Error details: {str(e)}')       


    def execute_sql_query(self, query: str):
        with self.engine.connect() as connection:
            result = connection.execute(query)
            return result