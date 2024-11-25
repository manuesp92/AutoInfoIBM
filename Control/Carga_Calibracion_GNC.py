
import pandas as pd
import pyodbc #Library to connect to Microsoft SQL Server
from datetime import datetime
from datetime import timedelta
from datetime import date
from datetime import timedelta,datetime
import datetime

server = "192.168.200.33,50020\cloud"
database = "Rumaos"
username = "gpedro" 
password = "s3rv1d0r"

login = [server,database,username,password]

#################################
tiempoInicio = pd.to_datetime("today")
#########
# Formating display of dataframes with comma separator for numbers
pd.options.display.float_format = "{:20,.0f}".format 
#########

try:
    db_conex = pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};\
        SERVER="+login[0]+";\
        DATABASE="+login[1]+";\
        UID="+login[2]+";\
        PWD="+ login[3]
    )
except Exception as e:
    listaErrores = e.args[1].split(".")
    exit()

hoy = datetime.datetime.now()
ayer = hoy - timedelta(days=1)

df = pd.read_sql('''

select 
UEN,FECHASQL AS Fecha,Turno,VTATOTVOL 
from EmpVenta 
where FECHASQL >= '2023-01-11' and CODPRODUCTO = 'gnc'
order by FECHASQL

  ''',db_conex)

df = df.convert_dtypes()
# Crea una columna 'mes' para identificar el mes de cada fecha
df['mes'] = df['Fecha'].dt.month

df_resultado = pd.DataFrame()

for uen in df['UEN'].unique():
    # Ordena el DataFrame por fecha
    subset_df = df[df['UEN'] == uen]
    subset_df = subset_df.sort_values(by=['Fecha','Turno'])
    subset_df['ventas_acumuladas_mes'] = subset_df.groupby('mes')['VTATOTVOL'].cumsum()
    df_resultado = pd.concat([df_resultado, subset_df], ignore_index=True)

import requests
import time
import pandas as pd
import json
from oauth2client.service_account import ServiceAccountCredentials
import gspread

scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
ruta2 ="C:/Informes/Control/calibraciongnc-85b0ca13aa62.json"

credenciales = ServiceAccountCredentials.from_json_keyfile_name(ruta2, scope)
cliente = gspread.authorize(credenciales)
sheet= cliente.open("Calibracion_GNC").get_worksheet_by_id(0)

def cargar(df, sheet):

    df['Fecha'] = pd.to_datetime(df['Fecha'])
    df['Fecha'] = df['Fecha'].dt.strftime('%Y-%m-%d')

    #sheet.insert_rows([df.columns.values.tolist()]+ df.values.tolist())

    sheet.append_rows([df.columns.values.tolist()]+ df.values.tolist())
    print('Carga completa')
    
bandera = True

if bandera == True:
    sheet.clear()
    #cargar(deudaSheet2022, sheet)
    cargar(df_resultado, sheet)