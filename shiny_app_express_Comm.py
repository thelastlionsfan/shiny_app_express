
"""
DataCraft: a single point for querying across Salesforce environments and SQL Servers
"""

from NiiForce import SOAPLoader, SOQLLoader, RESTFileLoader, MetaData, BULKLoaderv1, BULKLoaderv2,sfconn,sql_conn
import pandas as pd
import sqlalchemy
import sqlite3
import pyodbc
import duckdb
import json

# Express
from shiny.express import input, render, ui
from shiny import reactive, Session
from shiny.ui import page_navbar
from functools import partial
import shiny

# Global Variables for functions
sql_alc_eng = {'MSSQL':'mssql+pymssql','PostgreSQL':'postgresql+psycopg2','MySQL':'mysql+pymysql'}
login_resp,access_token,inst_url,session_dur,server_url = ['','','','','']
duckdb_df = pd.DataFrame([{'Data':None}])
soql_df = pd.DataFrame([{'Error':'e'}])
sql_df = pd.DataFrame([{'Error':'e'}])
SQL_Conn = ''

# Top Title + Page Navigation
ui.page_opts(
    title="DataCraft",  
    page_fn=partial(page_navbar, id="page"),  
)

sql_sidebar = ui.sidebar(position='left')
sf_sidebar = ui.sidebar(position='left')
duckdb_sidebar = ui.sidebar(position='left')

with ui.nav_panel("SQL"):
    ui.input_dark_mode()
    with ui.layout_sidebar():
        with sql_sidebar:
            # SQL Server Login
            ui.markdown("## SQL Server Login")
            ui.input_selectize("SQL_Server_Type","Server Type",['MSSQL','PostgreSQL','MySQL'],selected='MSSQL')
            ui.input_text("SQL_Server","SQL Server:")
            ui.input_text("SQL_DB","SQL Database (optional):")
            ui.input_text("SQL_UID","SQL User ID:")
            ui.input_password("SQL_PWD","SQL Password:")
            ui.input_action_button("SQL_Login","SQL Login",disabled=True)

            # SQL Querying
            ui.input_text_area("SQL_Query","SQL Query","SELECT TOP 10 * FROM PNM_PersonAccount")

            ui.input_action_button("SQL_Query_Run","Query!",disabled=True)

            # SQL Query Saving
            ui.input_text("SQL_Query_Tab","Table Name:")
            ui.input_action_button("SQL_Query_Tab_Save","Save Table",disabled=True)
    
        # Turning on the SQL Login button - only after password is provided
        @reactive.effect
        @reactive.event(input.SQL_PWD)
        def set_SQL_login_button_state():
            if input.SQL_Server() and input.SQL_UID() and input.SQL_PWD():
                # Need server URL/IP Address, User ID, and Password
                ui.update_action_button("SQL_Login", disabled=False)
            else:
                ui.update_action_button("SQL_Login", disabled=True)

        # Turning on the SQL Query button - only after Query Statement is provided
        @reactive.effect
        @reactive.event(input.SQL_Query)
        def set_SQL_query_button_state():
            if input.SQL_Query():
                ui.update_action_button("SQL_Query_Run", disabled=False)
            else:
                ui.update_action_button("SQL_Query_Run", disabled=True)

        # Turning on the SQL Query Saving button - only after Query Table Name is provided
        @reactive.effect
        @reactive.event(input.SQL_Query_Tab)
        def set_SQL_query_save_button_state():
            if input.SQL_Query_Tab():
                ui.update_action_button("SQL_Query_Tab_Save", disabled=False)
            else:
                ui.update_action_button("SQL_Query_Tab_Save", disabled=True)

        # Main Display Card for SQL
        with ui.card():
            ui.markdown("## SQL Server Playground")

            # Render code = returns to the main UI values in Python Coding format. Other options include @render.text, etc.
            # Reactive Event = The function below should run as a "reaction" to the "event" of the button being clicked, i.e. "SQL Login" button in this case
            @render.code
            @reactive.event(input.SQL_Login)
            def sql_login():
                # Database Connection

                # Need a little more explination on this if/else.
                if input.SQL_DB():
                    SQL_Eng = sqlalchemy.create_engine(f"{sql_alc_eng[input.SQL_Server_Type()]}://{input.SQL_UID()}:{input.SQL_PWD()}@{input.SQL_Server()}/{input.SQL_DB()}")
                else:
                    SQL_Eng = sqlalchemy.create_engine(f"{sql_alc_eng[input.SQL_Server_Type()]}://{input.SQL_UID()}:{input.SQL_PWD()}@{input.SQL_Server()}")
                try:
                    global SQL_Conn
                    SQL_Conn = SQL_Eng.connect()
                    conn_val = SQL_Conn.connection.is_valid
                    err_val = ''
                except Exception as e:
                    conn_val = False
                    err_val = e
                if err_val=='':
                    return f"SQL Connection live: {conn_val}."
                else:
                    return f"SQL Connection live: {conn_val} - Error:{err_val}."
            
            # Render data_frame = Returns a dataframe view 
            # Is this also where the data is being created and stored locally? It was my understanding we could query the data, have it saved locally and not "need" to create a table if we so choose.
            @render.data_frame
            @reactive.event(input.SQL_Query_Run)
            def sql_query_result():
                global sql_df
                try:
                # Database Querying
                    sql_df=pd.read_sql(input.SQL_Query(),SQL_Conn)

                # Display the query results in Shiny UI
                    return render.DataTable(sql_df)

                except Exception as e:
                    sql_df = pd.DataFrame([{'Error':e}])
                    return render.DataTable(sql_df)
                
            
            @render.code
            @reactive.event(input.SQL_Query_Tab_Save)
            def sql_query_save():
                global sql_df
                try:
                # DuckDb command to save sql dataframe into local memory as a named table for further use
                    duckdb.execute(f"CREATE OR REPLACE TABLE {input.SQL_Query_Tab()} as SELECT * FROM sql_df")

                # Display the query results in Shiny UI
                    return f"DuckDb Table '{input.SQL_Query_Tab()}' created"

                except Exception as e:
                    
                    return f"Error creating DuckDb Table '{input.SQL_Query_Tab()}': {e}"


with ui.nav_panel("Salesforce"):
    ui.input_dark_mode()
    with ui.layout_sidebar():
        with sf_sidebar:
            # Salesforce Login
            ui.markdown("## Salesforce Login")
            ui.input_selectize("SF_Env_Type","Environment Type",['sandbox','production'],selected='sandbox')
            ui.input_text("SF_Org_Url","Salesforce Org URL:")
            ui.input_text("SF_UID","Salesforce User ID:")
            ui.input_password("SF_PWD","Salesforce Password:")
            ui.input_text("SF_Sec_Token","Security Token:")
            ui.input_action_button("SF_Login","Salesforce Login",disabled=True)

            # Salesforce SOQL Querying
            ui.input_selectize("SOQL_Query_Type","Query Type",['BULK','REST'],selected='BULK')
            ui.input_text_area("SOQL_Query","SOQL Query","SELECT Id,Name FROM Account")

            ui.input_action_button("SOQL_Query_Run","Query!",disabled=True)

            # Salesforce SOQL Query Saving
            ui.input_text("SOQL_Query_Tab","Table Name:")
            ui.input_action_button("SOQL_Query_Tab_Save","Save Table",disabled=True)

        # Turning on the Salesforce Login button - only after password is provided
        @reactive.effect
        @reactive.event(input.SF_PWD)
        def set_SF_login_button_state():
            if input.SF_UID() and input.SF_PWD():
                # Needs Salesforce User ID and Password to login. Security Token and Org Url may not always be available or needed.
                ui.update_action_button("SF_Login", disabled=False)
            else:
                ui.update_action_button("SF_Login", disabled=True)

        # Turning on the SOQL Query button - only after Query Statement is provided
        @reactive.effect
        @reactive.event(input.SOQL_Query)
        def set_SOQL_button_state():
            if input.SOQL_Query():
                ui.update_action_button("SOQL_Query_Run", disabled=False)
            else:
                ui.update_action_button("SOQL_Query_Run", disabled=True)

        # Turning on the SOQL Query Saving button - only after Query Table Name is provided
        @reactive.effect
        @reactive.event(input.SOQL_Query_Tab)
        def set_SOQL_query_save_button_state():
            if input.SOQL_Query_Tab():
                ui.update_action_button("SOQL_Query_Tab_Save", disabled=False)
            else:
                ui.update_action_button("SOQL_Query_Tab_Save", disabled=True)

        with ui.card():
            # Main Display Card for Salesforce Login and SOQL Querying
            ui.markdown("## Salesforce Playground")
            @render.code
            @reactive.event(input.SF_Login)
            def sf_login():             
                # Salesforce Connection
                global login_resp,access_token,inst_url,session_dur,server_url
                try:
                    login_resp,access_token,inst_url,session_dur,server_url = sfconn.login(input.SF_Env_Type(),input.SF_UID(),input.SF_PWD(),input.SF_Sec_Token(),url=input.SF_Org_Url())
                    err_val = ''
                except Exception as e:
                    err_val = e
                if err_val=='':
                    return f"SF Connection live for {str(session_dur)} seconds."
                else:
                    return f"SF Connection not set - Error:{err_val}."
            
            @render.data_frame
            @reactive.event(input.SOQL_Query_Run)
            def soql_query_result():
                global login_resp,access_token,inst_url,session_dur,server_url,soql_df
                # Pulling SOQL Query into dataframe
                # Wouldn't this outcome be the same regardless of what type where chosen?
                try:
                    if input.SOQL_Query_Type()=="REST":
                        soql_df=SOQLLoader.SOQLRestPuller(access_token,inst_url,input.SOQL_Query())
                    else:
                        soql_df=SOQLLoader.SOQLBulkPuller(access_token,inst_url,input.SOQL_Query())

                # Display the query results in Shiny UI
                    return render.DataTable(soql_df)

                except Exception as e:
                    soql_df = pd.DataFrame([{'Error':e}])
                    return render.DataTable(soql_df)
            
            @render.code
            @reactive.event(input.SOQL_Query_Tab_Save)
            def soql_query_save():
                global soql_df
                try:
                # DuckDb command to save soql dataframe into local memory as a named table for further use
                    duckdb.execute(f"CREATE OR REPLACE TABLE {input.SOQL_Query_Tab()} as SELECT * FROM soql_df")

                # Display the query results in Shiny UI
                    return f"DuckDb Table '{input.SOQL_Query_Tab()}' created"

                except Exception as e:
                    return f"Error creating DuckDb Table '{input.SOQL_Query_Tab()}': {e}"
                
with ui.nav_panel("DuckDb"):
    ui.input_dark_mode()
    with ui.layout_sidebar():
        with duckdb_sidebar:
            # DuckDb SQL Access
            ui.markdown("## DuckDb Local SQL")
            ui.input_text_area("DuckDb_Query","Local Table Query:","SHOW ALL TABLES")

            ui.input_action_button("Run_DuckDb_Query","Run Local Query",disabled=True)

            # Saving Query Results to new named tables
            ui.input_text("DuckDb_Query_Tab","Table Name:")
            ui.input_action_button("DuckDb_Query_Tab_Save","Save Table",disabled=True)


        @reactive.effect
        @reactive.event(input.DuckDb_Query)
        def set_DuckDb_button_state():
            if input.DuckDb_Query():
                ui.update_action_button("Run_DuckDb_Query", disabled=False)
            else:
                ui.update_action_button("Run_DuckDb_Query", disabled=True)

        @reactive.effect
        @reactive.event(input.DuckDb_Query_Tab)
        def set_DuckDb_query_save_button_state():
            if input.DuckDb_Query_Tab():
                ui.update_action_button("DuckDb_Query_Tab_Save", disabled=False)
            else:
                ui.update_action_button("DuckDb_Query_Tab_Save", disabled=True)

        with ui.card():
            ui.markdown("## DuckDB: Local SQL Querying")

            @render.data_frame
            @reactive.event(input.Run_DuckDb_Query)
            def DuckDb_query_result():
                global duckdb_df
                try:
                # DuckDb Querying
                    duckdb_df=duckdb.sql(input.DuckDb_Query()).pl()

                # Display the query results in Shiny UI
                    return render.DataTable(duckdb_df)

                except Exception as e:
                    duckdb_df = pd.DataFrame([{'Error':e}])
                    return render.DataTable(duckdb_df)
            
            @render.code
            @reactive.event(input.DuckDb_Query_Tab_Save)
            def duckdb_query_save():
                global duckdb_df
                try:
                # DuckDb command to save soql dataframe into local memory as a named table for further use
                    duckdb.execute(f"CREATE OR REPLACE TABLE {input.DuckDb_Query_Tab()} as SELECT * FROM duckdb_df")

                # Display the query results in Shiny UI
                    return f"DuckDb Table '{input.DuckDb_Query_Tab()}' created"

                except Exception as e:
                    
                    return f"Error creating DuckDb Table '{input.DuckDb_Query_Tab()}': {e}"