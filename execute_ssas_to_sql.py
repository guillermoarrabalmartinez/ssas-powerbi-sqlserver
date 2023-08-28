from python.ssas_utils import ssas_connection
from decouple import config
from configuration.config import get_config
from python.sql_utils import SQLDatabase

############################################################################ 
# First define the parameters to get data from ssas
server = config("SSAS_MARKETRISK_SERVER")
database = config("SSAS_DATABASE")
dax_query = '''
//any valid DAX query
DEFINE
VAR MVE_table =
        FILTER (
            SUMMARIZECOLUMNS (
                'External Counterparty'[MVE Cash Included],
                'MVE'[reporting_date],
                'MVE'[Level],
                'MVE'[division],
                'MVE'[commodity],
                'MVE'[country],
                'MVE'[delivery_date],
                'MVE'[CPTY_CODE],
                'MVE'[book],
                "MVE in m", [MVE in m]
            ),
            NOT ( 'MVE'[commodity] IN { "INTEREST RATE", "CURRENCY" } )
        )EVALUATE
MVE_table
'''

############################################################################
# Create the connection instance
ssas = ssas_connection(server,database,dax_query)
# Create the dataframe
df = ssas.dataframe_from_ssas_dax()
# Rename columns appropiately (specific for this usecase)
df = ssas.rename_columns(df)

############################################################################
# Secondly define sql parameters to insert the dataframe into desired sql server table
# Invoke the get config from config.py 
configs = get_config()
connection_string = configs['SQL']['MARKETRISK_CONNECTION_STRING']
schema = configs['SQL']['MARKETRISK_SCHEMA']
table =  configs['SQL']['MARKETRISK_TABLE'] 

############################################################################
#SQL Instance
sql_conn = SQLDatabase(connection_string)
# Execute the insert into table
sql_conn.connect_and_load_to_sqlserver(df,table,schema)

############################################################################