# SSAS PowerBi instance to sql server
Repository that contains a simple python script to get connected to the Analysis Services Instance and execute a dax query. 
Once we have the dax query output as a pandas Dataframe insert the result into SQL server

You have to configure your own variables in a configuration\config.json file
````
{
    "SQL": {
        "MARKETRISK_CONNECTION_STRING": "mssql+pyodbc://yourservername/yourdatabasename?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server",
        "MARKETRISK_SCHEMA" : "yourschemaname",
        "MARKETRISK_TABLE" :  "yourtablename" 
    }
} 
````

# .env file creation
Just rename the .env.test file to .env and add your credentials. 
If you publish to a public code repo remember to keep in the .gitignore to avaid sharing your credentials

The idea come from that repo that I used to create the code:
https://github.com/yehoshuadimarsky/python-ssas
