import os
import math
import numpy as np
from DatosLogin import login
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

######## Despachos de Combustible con Tarjeta de Ayer
dataset = pd.read_sql('''
DECLARE @ayer DATETIME
	SET @ayer = DATEADD(day, -1, CAST(GETDATE() AS date))
	DECLARE @inicioMesActual DATETIME
	SET @inicioMesActual = DATEADD(month, DATEDIFF(month, 0, @ayer), 0)

        DECLARE @inicioMesAnterior DATETIME
        SET @inicioMesAnterior = DATEADD(M,-1,@inicioMesActual)

        --Divide por la cant de días del mes anterior y multiplica por la cant de días del
        --mes actual
        
        DECLARE @hoy DATETIME
        SET @hoy = DATEADD(DAY, DATEDIFF(DAY, 0, CURRENT_TIMESTAMP), 0)

        --Divide por la cantidad de días cursados del mes actual y multiplica por la cant
        --de días del mes actual

select d.*,c.NOMBRE from despapro as d left join PersonalUEN as C 
on d.CodPersonal = c.CODPERSONAL and d.UEN = c.UEN
where FECHASQL = @ayer and Tarjeta <> '' and Tarjeta <> 'IG99999' and volumen > 2


  '''      ,db_conex)
dataset = dataset.convert_dtypes()

# Convertir FECHADESPSQL a tipo datetime si no lo está
dataset['FECHADESPSQL'] = pd.to_datetime(dataset['FECHADESPSQL'])

# Ordenar el dataset por cliente y fecha para facilitar el cálculo de las diferencias de tiempo
dataset = dataset.sort_values(by=['UEN', 'TARJETA', 'FECHADESPSQL'])

# Calcular las diferencias de tiempo entre las filas consecutivas por cliente
dataset['time_diff'] = dataset.groupby(['UEN', 'TARJETA'])['FECHADESPSQL'].diff()

# Inicializar la columna de cliente repetido en False
dataset['nrocliente_repetido'] = False


# Nueva Condición 3: Si el CODPRODUCTO es GNC y GO < 2 horas
cond1 = (dataset['CODPRODUCTO'] == 'GNC') & (dataset['CODPRODUCTO'].shift() == 'GO') & (dataset['time_diff'] <= timedelta(hours=2)) 

# Nueva Condición 4: Si es GNC y NAFTA < 30 min y NAFTA > 10 min
cond2 = (dataset['CODPRODUCTO'] == 'GNC') & (dataset['CODPRODUCTO'].shift() == 'NS') & (dataset['time_diff'] <= timedelta(hours=2)) & (dataset['time_diff'] >= timedelta(minutes=30)) & (dataset['time_diff'] <= timedelta(minutes=1))

# Nueva Condición 3: Si el CODPRODUCTO es GNC y GO < 2 horas
cond3 = (dataset['CODPRODUCTO'] == 'GO') & (dataset['CODPRODUCTO'].shift() == 'NS') & (dataset['time_diff'] <= timedelta(hours=5))

# Nueva Condición 4: Si es GNC y NAFTA < 30 min y NAFTA > 10 min
cond4 = (dataset['CODPRODUCTO'] == 'NS') & (dataset['CODPRODUCTO'].shift() == 'NU') & (dataset['time_diff'] <= timedelta(hours=2))

# Nueva Condición 5: Si es el mismo CODPRODUCTO y mismo AFORADOR menor a 1 hora y mayor a 3 minutos
cond5 = (dataset['CODPRODUCTO'] == dataset['CODPRODUCTO'].shift()) & (dataset['AFORADOR'] == dataset['AFORADOR'].shift()) & (dataset['time_diff'] <= timedelta(hours=1)) & (dataset['time_diff'] >= timedelta(minutes=5))  & (dataset['VOLUMEN'] <= 70) 

# Nueva Condición 6: Si es el mismo CODPRODUCTO y distinto AFORADOR menor a 1 hora
cond6 = (dataset['CODPRODUCTO'] == dataset['CODPRODUCTO'].shift()) & (dataset['AFORADOR'] != dataset['AFORADOR'].shift()) & (dataset['time_diff'] <= timedelta(hours=1)) & (dataset['VOLUMEN'] <= 70) 

# Nueva Condición 5: Si es el mismo CODPRODUCTO y mismo AFORADOR menor a 1 hora y mayor a 3 minutos
cond7 = (dataset['CODPRODUCTO'] != dataset['CODPRODUCTO'].shift()) & (dataset['AFORADOR'] == dataset['AFORADOR'].shift()) & (dataset['time_diff'] <= timedelta(hours=2)) 


# Aplicar todas las condiciones para marcar los clientes repetidos
repeated_conditions = cond1 | cond2 | cond3 | cond4 | cond5 | cond6 | cond7

# Marcar como repetidos los despachos actuales y los anteriores
dataset.loc[repeated_conditions, 'nrocliente_repetido'] = True
dataset.loc[repeated_conditions.shift(-1).fillna(False), 'nrocliente_repetido'] = True

dataset = dataset.loc[(dataset['nrocliente_repetido'] == True) ,:]

dataset = dataset.reindex(columns= ['UEN','FECHADESPSQL','CODPRODUCTO','AFORADOR','VOLUMEN','TARJETA','NOMBRE','nrocliente_repetido'])

def _estiladorVtaTituloP(df, list_Col_Num, list_Col_Num0, list_Col_Perc, titulo, evitarTotal):
    """
    Esta función devolverá un DataFrame estilizado que debe asignarse a una variable.
    ARGS:
        df: DataFrame que será estilizado.
        list_Col_Num: Lista de columnas numéricas que serán formateadas con
        cero decimales y separador de miles.
        list_Col_Perc: Lista de columnas numéricas que serán formateadas 
        como porcentaje.
        titulo: Cadena para el título de la tabla.
    """
    # Lista de colores
    colors = [
        'lightblue', 'lightgreen', 'lightcoral', 'lightpink', 'lightgoldenrodyellow',
        'lightseagreen', 'lightsalmon', 'lightcyan', 'lightgray', 'lightskyblue',
        'lightsteelblue', 'lightyellow', 'plum', 'peachpuff'
    ]
    
    # Eliminar los espacios en blanco al inicio y al final de los valores en la columna UEN
    df['UEN'] = df['UEN'].str.strip()
    
    # Crear un diccionario para asignar un color a cada UEN
    unique_uens = df['UEN'].unique()
    color_map = {uen: colors[i % len(colors)] for i, uen in enumerate(unique_uens)}
    
    # Función para aplicar colores basados en UEN a toda la fila
    def color_groups(row):
        return [f'background-color: {color_map.get(row.UEN, "")}']*len(row)

    resultado = df.style \
        .format("{0:,.0f}", subset=list_Col_Num0) \
        .format("{0:,.2f}", subset=list_Col_Num) \
        .format("{:,.2%}", subset=list_Col_Perc) \
        .hide(axis=0) \
        .set_caption(
            titulo
            + "<br>"
            + ((tiempoInicio - pd.to_timedelta(1, "days")).strftime("%d/%m/%y"))
            + "<br>") \
        .set_properties(subset=list_Col_Perc + list_Col_Num + list_Col_Num0,
                        **{"text-align": "center", "width": "100px"}) \
        .set_properties(border="2px solid black") \
        .set_table_styles([
            {"selector": "caption", 
             "props": [
                 ("font-size", "20px"),
                 ("text-align", "center")
             ]
            },
            {"selector": "th", 
             "props": [
                 ("text-align", "center"),
                 ("background-color", "black"),
                 ("color", "white")
             ]
            }
        ]) \
        .apply(lambda x: ["background: black" if x.name == "colTOTAL" else "" for i in x], axis=1) \
        .apply(lambda x: ["color: white" if x.name == "colTOTAL" else "" for i in x], axis=1) \
        .apply(color_groups, axis=1)

    return resultado

##Columnas

numCols0=['VOLUMEN']
numCols=[]


## Columnas porcentajes
percColsPen = []
#percColsPen = ['CALIBRACION']

alerta= _estiladorVtaTituloP(dataset, numCols, numCols0, percColsPen, 'Posibles Fraudes RedMas',0)

ubicacion = str(pathlib.Path(__file__).parent)+"\\"
nombreCalibracionGNC = "Fraude_RM.png"
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

df_to_image(alerta, ubicacion, nombreCalibracionGNC)

#### ARCHIVO EXCEL

# Definimos la ubicación del archivo 
ubicacionExcel = str(pathlib.Path(__file__).parent)+"\\"

# Definimos el nombre del archivo
nombreExcel = "Fraude_RM.xlsx"

# Generamos el archivo y lo guardamos en la ubicación y con el nombre que definimos más arriba

def df_to_Excel(df, ubicacion, nombre):
   
    if os.path.exists(ubicacion+nombre):
        os.remove(ubicacion+nombre)
        df.to_excel(ubicacion+nombre)
    else:
        df.to_excel(ubicacion+nombre)

df_to_Excel(alerta, ubicacionExcel, nombreExcel)
