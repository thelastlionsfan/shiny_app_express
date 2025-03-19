import shiny
import pandas as pd
import json
import sqlalchemy
import sqlite3
import pyodbc
from NiiForce import SOAPLoader, SOQLLoader, RESTFileLoader, MetaData, BULKLoaderv1, BULKLoaderv2,sfconn,sql_conn
import duckdb

# Express
from shiny import reactive, Session
from shiny.express import input, render, ui


sql_alc_eng = {'MSSQL':'mssql+pymssql','PostgreSQL':'postgresql+psycopg2','MySQL':'mysql+pymysql'}

ui.panel_title("DataCraft")

sql_sidebar = ui.sidebar(position='left')
with sql_sidebar:
    ui.input_dark_mode()
    #ui.input_text("txt_in", "Type something here:")
    ui.markdown("## SQL Server Login")
    ui.input_selectize("SQL_Server_Type","Server Type",['MSSQL','PostgreSQL','MySQL'],selected='MSSQL')
    ui.input_text("SQL_Server","SQL Server:")
    ui.input_text("SQL_DB","SQL Database (optional):")
    ui.input_text("SQL_UID","SQL User ID:")
    ui.input_password("SQL_PWD","SQL Password:")
    ui.input_action_button("SQL_Login","SQL Login",disabled=True)

    ui.input_text_area("SQL_Query","SQL Query","SELECT TOP 10 * FROM PNM_PersonAccount")

    ui.input_action_button("SQL_Query_Run","Query!",disabled=True)

#with ui.popover(id="SQL_Login"):

@reactive.effect
@reactive.event(input.SQL_PWD)
def set_SQL_login_button_state():
    if input.SQL_Server() and input.SQL_UID() and input.SQL_PWD():
        ui.update_action_button("SQL_Login", disabled=False)
    else:
        ui.update_action_button("SQL_Login", disabled=True)

SQL_Conn = ''
@reactive.effect
@reactive.event(input.SQL_Query)
def set_SQL_login_button_state():
    if input.SQL_Query():
        ui.update_action_button("SQL_Query_Run", disabled=False)
    else:
        ui.update_action_button("SQL_Query_Run", disabled=True)

with ui.card():
    @render.code
    @reactive.event(input.SQL_Login)
    def sql_login():
        #if st.session_state['SQL_Login_click']:
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
            return f"SQL Connection live:'{conn_val}'."
        else:
            return f"SQL Connection live:'{conn_val}' - Error:{err_val}."
    
    @render.data_frame
    @reactive.event(input.SQL_Query_Run)
    def sql_query_result():
        try:
        # Database Connection
            sql_df=pd.read_sql(input.SQL_Query(),SQL_Conn)

        # Display the query results in Streamlit UI
            return render.DataTable(sql_df)

        except Exception as e:
            return render.DataTable(pd.DataFrame({'Error':e}))
            #st.error(f"Error executing query: {e}")


# Sidebar for SF Connection
salesforce_sidebar = ui.sidebar(position='right')
with salesforce_sidebar:
    ui.input_dark_mode()
    ui.markdown("## Salesforce Login")
    ui.input_text("SF_Username", "Salesforce Username:")
    ui.input_password("SF_Password", "Salesforce Password:")
    ui.input_text("SF_Token", "Security Token:")
    ui.input_action_button("SF_Login", "Salesforce Login", disabled=True)
    
    ui.input_text_area("SF_Query", "SOQL Query", "SELECT Id, Name FROM Account LIMIT 10")
    ui.input_action_button("SF_Query_Run", "Query!", disabled=True)

@reactive.effect
@reactive.event(input.SF_Password)
def set_SF_login_button_state():
    if input.SF_Username() and input.SF_Password() and input.SF_Token():
        ui.update_action_button("SF_Login", disabled=False)
    else:
        ui.update_action_button("SF_Login", disabled=True)

SF_Conn = ''

@reactive.effect
@reactive.event(input.SF_Query)
def set_SF_query_button_state():
    if input.SF_Query():
        ui.update_action_button("SF_Query_Run", disabled=False)
    else:
        ui.update_action_button("SF_Query_Run", disabled=True)

# Handle Salesforce Login
with ui.card():
    @render.code
    @reactive.event(input.SF_Login)
    def sf_login():
        try:
            global SF_Conn
            SF_Conn = Salesforce(username=input.SF_Username(),
                                 password=input.SF_Password(),
                                 security_token=input.SF_Token())
            return "Salesforce Connection: Successful"
        except Exception as e:
            return f"Salesforce Connection Failed: {e}"
    
    @render.data_frame
    @reactive.event(input.SF_Query_Run)
    def sf_query_result():
        try:
            if SF_Conn:
                query_result = SF_Conn.query(input.SF_Query())
                df = pd.DataFrame(query_result['records']).drop(columns='attributes', errors='ignore')
                return render.DataTable(df)
            else:
                return render.DataTable(pd.DataFrame({'Error': ['Not connected to Salesforce']}))
        except Exception as e:
            return render.DataTable(pd.DataFrame({'Error': [str(e)]}))
