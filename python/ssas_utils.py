import python.ssas_api as ssas_api
from decouple import config
import re

class ssas_connection:
    def __init__(self,server,database,dax_query):
        self.server = server
        self.database = database
        self.dax_query = dax_query

    def dataframe_from_ssas_dax(self):
        conn =  ssas_api.set_conn_string(
            server = self.server,
            db_name =  self.database,
            username = config("SSAS_USERNAME"),
            password= config("SSAS_PASSWORD")
        )    
        df = ssas_api.get_DAX(connection_string=conn, dax_string=self.dax_query)
        return df
    
    def headers_extract_between_brackets(self, column_name):
        # Handle the specific column name condition
        if column_name == 'External Counterparty[MVE Cash Included]':
            return 'cash_included'

        match = re.search(r'\[(.*?)\]', column_name)
        if match:
            return match.group(1).lower()  # Convert to lowercase
        else:
            return column_name
        
    def rename_columns(self,df):
        df.columns = df.columns.map(self.headers_extract_between_brackets)
        return df
    
    