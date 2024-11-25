import os
import math
import numpy as np
from PIL import Image
import pandas as pd
from pandas.api.types import CategoricalDtype
import dataframe_image as dfi
import pyodbc #Library to connect to Microsoft SQL Server
import sys
import pathlib
from datetime import datetime
from datetime import timedelta
from datetime import date
from datetime import timedelta,datetime
import datetime
from calendar import monthrange
import logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        , level=logging.INFO
)
logger = logging.getLogger(__name__)

# SQL SERVER Cloud
server = "192.168.200.33,50020\cloud"
database = "Rumaos"
username = "gpedro" 
password = "s3rv1d0r"

login = [server,database,username,password]


tiempoInicio = pd.to_datetime("today")

# Formating display of dataframes with comma separator for numbers
pd.options.display.float_format = "{:20,.0f}".format 


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


# SQL SERVER ETL MATIAS 
server2 = "192.168.200.44\CLOUD"
database2 = "Test_Rumaos"
username2 = "jbriffe" 
password2 = "t3GPnmn4"

login2 = [server2,database2,username2,password2]


try:
    db_conex2 = pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};\
        SERVER="+login2[0]+";\
        DATABASE="+login2[1]+";\
        UID="+login2[2]+";\
        PWD="+ login2[3]
    )
except Exception as e:
    listaErrores = e.args[1].split(".")
    logger.error("\nOcurrió un error al conectar a SQL Server: ")
    for i in listaErrores:
        logger.error(i)
        exit()

# Obtenemos el volumen total despachado en las YPF, sin tomar en cuenta 'GO' ni 'GNC' y sin considerar las operaciones de YER
df_Volumen_APPYPF = pd.read_sql('''
DECLARE @InicioDeMesActual DATE
SET @InicioDeMesActual = DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()), 0)

DECLARE @hoy DATETIME
SET @hoy = DATEADD(DAY, DATEDIFF(DAY, 0, CURRENT_TIMESTAMP), 0)

SELECT T.UEN, SUM(D.VOLUMEN) as 'VOLUMEN APPYPF'
FROM [MercadoPago].[dbo].[Transacciones]  as T
JOIN [Rumaos].[dbo].[DESPAPRO] as D
ON T.UEN = D.UEN
AND T.IDDESPACHO = D.IDMOVIM
WHERE D.FECHASQL >= @inicioDeMesActual
AND D.FECHASQL < @hoy
AND D.CODPRODUCTO NOT IN ('GO', 'GNC')
AND T.AppYPF = 1
GROUP BY T.UEN
''',db_conex)

df_Volumen_APPYPF = df_Volumen_APPYPF.convert_dtypes()


# Obtenemos el volumen total despachado en las YPF, sin tomar en cuenta 'GO' ni 'GNC' y sin considerar las operaciones de YER

df_Volumen_H_Despachos = pd.read_sql('''
DECLARE @InicioDeMesActual DATE
SET @InicioDeMesActual = DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()), 0)

DECLARE @hoy DATETIME
SET @hoy = DATEADD(DAY, DATEDIFF(DAY, 0, CURRENT_TIMESTAMP), 0)

SELECT UEN, SUM(VOLUMEN) AS 'VOLUMEN TOTAL H_Despachos' 
FROM [Test_Rumaos].[dbo].[H_Despachos]
WHERE UEN IN (
             'LAMADRID'
            ,'AZCUENAGA'
            ,'PERDRIEL'
            ,'PERDRIEL2'
            ,'PUENTE OLIVE'
            ,'SAN JOSE'
        )
AND DESCRIPCION NOT LIKE '%YER%' AND DESCRIPCION NOT LIKE '%YPF EN RUTA%'
AND CODPRODUCTO NOT IN ('GO', 'GNC')
AND FECHASQL >= @InicioDeMesActual AND FECHASQL <@hoy
GROUP BY UEN
''',db_conex2)

df_Volumen_H_Despachos = df_Volumen_H_Despachos.convert_dtypes()


# Mergeamos los dos DF
df_Volumen_H_Despachos = df_Volumen_H_Despachos.merge(df_Volumen_APPYPF,on=['UEN'],how='outer')
df_Volumen_H_Despachos = df_Volumen_H_Despachos.fillna(0)


## Creo la columna 'Penetracion App YPF'
df_Volumen_H_Despachos['Penetracion App YPF'] = df_Volumen_APPYPF['VOLUMEN APPYPF'] / df_Volumen_H_Despachos['VOLUMEN TOTAL H_Despachos']


#Creo totales
df_Volumen_H_Despachos.loc["colTOTAL"]= pd.Series(
    df_Volumen_H_Despachos.sum(numeric_only=True)
    , index=['VOLUMEN APPYPF',"VOLUMEN TOTAL H_Despachos"]
)
df_Volumen_H_Despachos.fillna({"UEN":"TOTAL"}, inplace=True)


#Creo totales de Penetracion App YPF
tasa = (df_Volumen_H_Despachos.loc["colTOTAL","VOLUMEN APPYPF"] / (df_Volumen_H_Despachos.loc["colTOTAL","VOLUMEN TOTAL H_Despachos"]))
df_Volumen_H_Despachos.fillna({'Penetracion App YPF':tasa}, inplace=True)


#Elimino Columnas Que no entraran en el informe
df_Volumen_H_Despachos = df_Volumen_H_Despachos.reindex(columns=['UEN','Penetracion App YPF'])
df_VentasYPFAPPREPORTE=df_Volumen_H_Despachos


def _estiladorVtaTitulo(df, list_Col_Num, list_Col_Perc,colcaract, titulo):
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
        .format("{0:,.0f}", subset=list_Col_Num) \
        .format("{:,.2%}", subset=list_Col_Perc) \
        .hide(axis=0) \
        .set_caption(
            titulo
            + "<br>"
            + ((tiempoInicio-pd.to_timedelta(1,"days")).strftime("%d/%m/%y"))
            + "<br>") \
        .set_properties(subset= list_Col_Perc + list_Col_Num + colcaract
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
    
    return resultado

### COLUMNAS Con Numeros Enteros
numCols = []

### COLUMNAS con Porcentajes
percColsPen = ['Penetracion App YPF']

#### COLUMNAS Con caracteres
colcaract = []

###### Aplico el formato elegido a la imagen
df_Volumen_H_Despachos = _estiladorVtaTitulo(df_Volumen_H_Despachos,numCols,percColsPen,colcaract, "INFO Penetracion App YPF por Estacion")

###### DEFINO NOMBRE Y UBICACION DE DONDE SE VA A GUARDAR LA IMAGEN

ubicacion = str(pathlib.Path(__file__).parent)+"\\"
nombrePen = "Info_Penetracion_UEN.png"

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

df_to_image(df_Volumen_H_Despachos, ubicacion, nombrePen)




#############
# Timer
tiempoFinal = pd.to_datetime("today")
logger.info(
    "\nInfo Volumen de Ventas"
    + "\nTiempo de Ejecucion Total: "
    + str(tiempoFinal-tiempoInicio)
)

