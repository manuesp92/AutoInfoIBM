import os
import math
import numpy as np
#from Conectores import conectorMSSQL
from PIL import Image
import pandas as pd
from pandas.api.types import CategoricalDtype
import dataframe_image as dfi
import pyodbc #Library to connect to Microsoft SQL Server
from DatosLogin import login #Holds connection data to the SQL Server
import sys
import pathlib
from datetime import datetime
from datetime import timedelta
sys.path.insert(0,str(pathlib.Path(__file__).parent.parent))
import logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        , level=logging.INFO
)
logger = logging.getLogger(__name__)
import datetime
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
    logger.error("\nOcurrió un error al conectar a SQL Server: ")
    for i in listaErrores:
        logger.error(i)
    exit()
hoy = datetime.datetime.now()
ayer = hoy - timedelta(days=1)
primer_dia_mes = hoy.replace(day=1)

### Ventas GNC
sheet_id='1V3kYi6BZXfMHMhM9krJMt1V-kl2cuwo-8XvBQuPqxq8'
hoja='Calibracion_GNC'
gsheet_url_VtasGnc = "https://docs.google.com/spreadsheets/d/{}/gviz/tq?tqx=out:csv&sheet={}".format(sheet_id, hoja)
df_VtasGnc=pd.read_csv(gsheet_url_VtasGnc)
df_VtasGnc = df_VtasGnc.convert_dtypes()
df_VtasGnc['Fecha'] = pd.to_datetime(df_VtasGnc['Fecha'], format='%Y-%m-%d')
df_VtasGnc= df_VtasGnc.loc[(df_VtasGnc["Fecha"] <= ayer.strftime('%Y-%m-%d'))& (df_VtasGnc["Fecha"] >= primer_dia_mes.strftime('%Y-%m-%d')),:]
df_VtasGnc['VTATOTVOL'] = pd.to_numeric(df_VtasGnc['VTATOTVOL'].str.replace(',', '.'), errors='coerce')
df_VtasGnc['ventas_acumuladas_mes'] = pd.to_numeric(df_VtasGnc['ventas_acumuladas_mes'].str.replace(',', '.'), errors='coerce')
df_VtasGnc = df_VtasGnc.applymap(lambda x: x.strip() if isinstance(x, str) else x)

### Puente Medicion
sheet_id='1V3kYi6BZXfMHMhM9krJMt1V-kl2cuwo-8XvBQuPqxq8'
hoja='Puente_Medicion'
gsheet_url_Puente = "https://docs.google.com/spreadsheets/d/{}/gviz/tq?tqx=out:csv&sheet={}".format(sheet_id, hoja)
df_PuenteMed=pd.read_csv(gsheet_url_Puente)
df_PuenteMed = df_PuenteMed.convert_dtypes()
df_PuenteMed['Fecha'] = pd.to_datetime(df_PuenteMed['Fecha'], format='%d/%m/%Y')
df_PuenteMed= df_PuenteMed.loc[(df_PuenteMed["Fecha"] <= ayer.strftime('%Y-%m-%d'))& (df_PuenteMed["Fecha"] >= primer_dia_mes.strftime('%Y-%m-%d')),:]
df_PuenteMed = df_PuenteMed.applymap(lambda x: x.strip() if isinstance(x, str) else x)

### Reporte Diario

# Ventas GNC Puente Medicion
df_PuenteMed_Ayer  = df_PuenteMed.loc[(df_PuenteMed["Fecha"] == (ayer- timedelta(days=1)).strftime('%Y-%m-%d'))]
df_PuenteMed_Ayer = df_PuenteMed_Ayer.reindex(columns=['UEN','Puente Medicion'])
df_PuenteMed_Ayer['Puente Medicion'] = pd.to_numeric(df_PuenteMed_Ayer['Puente Medicion'].str.replace(',', '.'), errors='coerce')

# Ventas GNC Sges
df_VtasGnc_Diario = df_VtasGnc.loc[(df_VtasGnc["Fecha"] >= (ayer- timedelta(days=1)).strftime('%Y-%m-%d'))]
df_VtasGnc_Diario = df_VtasGnc_Diario.loc[~((df_VtasGnc_Diario["Fecha"] == (ayer- timedelta(days=1)).strftime('%Y-%m-%d')) & (df_VtasGnc_Diario["Turno"] == 1))]
df_VtasGnc_Diario = df_VtasGnc_Diario.loc[~((df_VtasGnc_Diario["Fecha"] == ayer.strftime('%Y-%m-%d')) & (df_VtasGnc_Diario["Turno"] == 2))]
df_VtasGnc_Diario = df_VtasGnc_Diario.loc[~((df_VtasGnc_Diario["Fecha"] == ayer.strftime('%Y-%m-%d')) & (df_VtasGnc_Diario["Turno"] == 3))]
df_VtasGnc_Diario = df_VtasGnc_Diario.reindex(columns=['UEN','Fecha','Turno','VTATOTVOL'])
df_VtasGnc_Diario = df_VtasGnc_Diario.groupby('UEN')['VTATOTVOL'].sum().reset_index()

df_consolidado_Ayer=df_VtasGnc_Diario.merge(df_PuenteMed_Ayer,on='UEN',how='outer')

df_consolidado_Ayer['CALIBRACION']=(df_consolidado_Ayer['VTATOTVOL']/df_consolidado_Ayer['Puente Medicion'])-1

df_consolidado_Ayer = df_consolidado_Ayer.reindex(columns= ['UEN','CALIBRACION'])

#### Reporte Acumulado Mensual

# Ventas GNC SGES
df_VtasGnc_Mes = df_VtasGnc.loc[~((df_VtasGnc["Fecha"] == primer_dia_mes.strftime('%Y-%m-%d')) & (df_VtasGnc["Turno"] == 1))]
df_VtasGnc_Mes['Ventas Acumuladas'] = df_VtasGnc_Mes.groupby('UEN')['VTATOTVOL'].cumsum()
df_VtasGnc_Mes = df_VtasGnc_Mes.loc[((df_VtasGnc_Mes["Fecha"] == ayer.strftime('%Y-%m-%d')) & (df_VtasGnc_Mes["Turno"] == 1))]


df_VtasGnc_Mes = df_VtasGnc_Mes.loc[((df_VtasGnc_Mes["Fecha"] == ayer.strftime('%Y-%m-%d')) & (df_VtasGnc_Mes["Turno"] == 1))]
df_VtasGnc_Mes=df_VtasGnc_Mes.reindex(columns=['UEN','ventas_acumuladas_mes'])

# Ventas GNC Puente de Medicion
df_PuenteMed_Mes = df_PuenteMed.loc[(df_PuenteMed["Fecha"] <= (ayer- timedelta(days=1)).strftime('%Y-%m-%d'))]
df_PuenteMed_Mes['Puente Medicion'] = pd.to_numeric(df_PuenteMed_Mes['Puente Medicion'].str.replace(',', '.'), errors='coerce')

df_PuenteMed_Mes['Ventas Acumuladas'] = df_PuenteMed_Mes.groupby('UEN')['Puente Medicion'].cumsum()
df_PuenteMed_Mes = df_PuenteMed_Mes.loc[df_PuenteMed_Mes["Fecha"] == (ayer- timedelta(days=1)).strftime('%Y-%m-%d')]
df_PuenteMed_Mes = df_PuenteMed_Mes.reindex(columns=['UEN','Ventas Acumuladas'])  
df_Calibracion_Mes = df_VtasGnc_Mes.merge(df_PuenteMed_Mes,on=['UEN'],how='outer')

df_Calibracion_Mes['CALIBRACION'] = (df_Calibracion_Mes['ventas_acumuladas_mes']/df_Calibracion_Mes['Ventas Acumuladas'])-1

df_Calibracion_Mes = df_Calibracion_Mes.reindex(columns= ['UEN','CALIBRACION'])

def _estiladorVtaTituloP(df,list_Col_Num, list_Col_Num0,list_Col_Perc, titulo, evitarTotal):
    """
This function will return a styled dataframe that must be assign to a variable.
ARGS:
    df: Dataframe that will be styled.
    list_Col_Num: List of numeric columns that will be formatted with
    zero decimals and thousand separator.
    list_Col_Perc: List of numeric columns that will be formatted 
    as percentage.
    titulo: String for the table caption.
    """
    resultado = df.style \
        .format("{0:,.0f}", subset=list_Col_Num0) \
        .format("{0:,.2f}", subset=list_Col_Num) \
        .format("{:,.2%}", subset=list_Col_Perc) \
        .hide(axis=0) \
        .set_caption(
            titulo
            + "<br>"
            + ((primer_dia_mes).strftime("%d/%m/%y"))
            + " - "
            + ((tiempoInicio-pd.to_timedelta(1,"days")).strftime("%d/%m/%y"))
            + "<br>") \
        .set_properties(subset= list_Col_Perc + list_Col_Num + list_Col_Num0
            , **{"text-align": "center", "width": "100px"}) \
        .set_properties(border= "2px solid black") \
        .set_table_styles([
            {"selector": "caption", 
                "props": [
                    ("font-size", "20px")
                    ,("text-align", "center")
                ]
            }
            , {"selector": "th", 
                "props": [
                    ("text-align", "center")
                    ,("background-color","black")
                    ,("color","white")
                ]
            }
        ]) \
        .apply(lambda x: ["background: black" if x.name == "colTOTAL" 
            else "" for i in x]
            , axis=1) \
        .apply(lambda x: ["color: white" if x.name == "colTOTAL" 
            else "" for i in x]
            , axis=1)
    
    evitarTotales = df.index.get_level_values(0) 
    if evitarTotal==1:
        subset_columns = pd.IndexSlice[evitarTotales[:-1],list_Col_Perc]
    else:
        subset_columns = pd.IndexSlice[list_Col_Perc]

        

    resultado= resultado.applymap(table_color,subset=subset_columns)
    
    return resultado

def table_color(val):
    """
    Takes a scalar and returns a string with
    the css property `'color: red'` for less than 60 marks, green otherwise.
    """
    if pd.notnull(val) and val > 0.0 and val < 0.1:
        color = 'blue'
    else:
        color = 'red'
    return 'color: % s' % color
##Columnas sin decimales
numCols0 = []
numCols1=[]
##Columnas con decimales

numCols = []

num=[]

## Columnas porcentajes
percColsPen = ['CALIBRACION']

alerta_Mes = _estiladorVtaTituloP(df_Calibracion_Mes, numCols, numCols0, percColsPen, 'INFO Calibracion GNC Alerta',0)


alerta_Ayer = _estiladorVtaTituloP(df_consolidado_Ayer, numCols, numCols0, percColsPen, 'INFO Calibracion GNC Alerta',0)




ubicacion = str(pathlib.Path(__file__).parent)+"\\"
nombreCalibracionGNC_Mes = "Calibracion_GNC_Acumulado.png"
nombreCalibracionGNC_Ayer = "Calibracion_GNC_Ayer.png"
def df_to_image(df, ubicacion, nombre):
    """
    Esta función usa las biblioteca "dataframe_Image as dfi" y "os" para 
    generar un archivo .png de un dataframe. Si el archivo ya existe, este será
    reemplazado por el nuevo archivo.

    Args:
        df: dataframe a convertir
         ubicacion: ubicacion local donde se quiere grabar el archivo
          nombre: nombre del archivo incluyendo extensión .png (ej: "hello.png")

    """
    if os.path.exists(ubicacion+nombre):
        os.remove(ubicacion+nombre)
        dfi.export(df, ubicacion+nombre)
    else:
        dfi.export(df, ubicacion+nombre)

df_to_image(alerta_Mes, ubicacion, nombreCalibracionGNC_Mes)
df_to_image(alerta_Ayer, ubicacion, nombreCalibracionGNC_Ayer)


