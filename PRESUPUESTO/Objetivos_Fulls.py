import os
import math
import numpy as np
from DatosLogin import login
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
from datetime import date
import datetime
from datetime import timedelta,datetime
sys.path.insert(0,str(pathlib.Path(__file__).parent.parent))
import logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        , level=logging.INFO
)
logger = logging.getLogger(__name__)

server = "192.168.200.33,50020\cloud"
database = "Rumaos"
username = "mmagistretti" 
password = "R3dmer0s#r"

login = [server,database,username,password]
#################################

tiempoInicio = pd.to_datetime("today")
#########
# Formating display of dataframes with comma separator for EUmbers
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

hoy = datetime.now()
ayer = hoy - timedelta(days=1)
primer_dia_mes = ayer.replace(day=1)

from datetime import datetime, timedelta
import calendar
# Obtener la fecha actual
fecha_actual = datetime.today() - timedelta(days=1)

# Calcular días transcurridos en el mes actual
dias_transcurridos = fecha_actual.day

# Calcular el número total de días del mes actual
dias_totales_mes = calendar.monthrange(fecha_actual.year, fecha_actual.month)[1]


    ###PRESUPUESTO  
sheet_id='1ofJ38O2-dLfw7TZOxeTDYUzlWgl6hN2iMLZCMscTgGo'
hoja='OBJETIVOS'
gsheet_url_presupuesto = "https://docs.google.com/spreadsheets/d/{}/gviz/tq?tqx=out:csv&sheet={}".format(sheet_id, hoja)
df_presupuesto=pd.read_csv(gsheet_url_presupuesto)
df_presupuesto = df_presupuesto.convert_dtypes()
df_presupuesto['FECHA'] = pd.to_datetime(df_presupuesto['FECHA'], format='%d/%m/%Y')
df_presupuesto= df_presupuesto.loc[(df_presupuesto["FECHA"] <= ayer.strftime('%Y-%m-%d'))& (df_presupuesto["FECHA"] >= primer_dia_mes.strftime('%Y-%m-%d')),:]

tiquet_Full = pd.read_sql('''
DECLARE @ayer DATETIME
SET @ayer = DATEADD(day, -1, CAST(GETDATE() AS date))

DECLARE @inicioMesActual DATETIME
SET @inicioMesActual = DATEADD(month, DATEDIFF(month, 0, @ayer), 0)

DECLARE @inicioMesAnterior DATETIME
SET @inicioMesAnterior = DATEADD(M, -1, @inicioMesActual)

DECLARE @hoy DATETIME
SET @hoy = DATEADD(DAY, DATEDIFF(DAY, 0, CURRENT_TIMESTAMP), 0)

-- Obtener importe y cantidad de tiquets del mes actual y del mes anterior
SELECT 
    u.UEN, 
    COALESCE(a.CantTiquetsActual, 0) AS CantTiquetsActual, 
    COALESCE(a.ImporteTiquetActual, 0) AS ImporteTiquetActual,
    COALESCE(p.ObjetivoTiquet, 0) AS ObjetivoTiquet, 
    COALESCE(p.ObjetivoImporte, 0) AS ObjetivoImporte
FROM
    (SELECT DISTINCT UEN 
     FROM fac001 
     WHERE descdepto = 'SERVICOMPRAS' 
       AND UEN IN ('AZCUENAGA', 'PERDRIEL', 'PERDRIEL2', 'SAN JOSE', 'PUENTE OLIVE', 'LAMADRID')) u
LEFT JOIN
    (SELECT 
         UEN, 
         COUNT(DISTINCT ID) AS CantTiquetsActual, 
         SUM(Imptotal) AS ImporteTiquetActual
     FROM fac001 
     WHERE descdepto = 'SERVICOMPRAS' 
       AND FECHASQL >= @inicioMesActual AND FECHASQL < @hoy
     GROUP BY UEN) a
ON u.UEN = a.UEN
LEFT JOIN
    (SELECT 
         UEN, 
         COUNT(DISTINCT ID)*1.1 AS ObjetivoTiquet, 
         SUM(Imptotal)*1.1 AS ObjetivoImporte
     FROM fac001 
     WHERE descdepto = 'SERVICOMPRAS' 
       AND FECHASQL >= @inicioMesAnterior AND FECHASQL < @inicioMesActual
     GROUP BY UEN) p
ON u.UEN = p.UEN
ORDER BY u.UEN;

  ''',db_conex)
tiquet_Full = tiquet_Full.convert_dtypes()
tiquet_Full['UEN'] = tiquet_Full['UEN'].str.rstrip()


ventas_Full = pd.read_sql('''
DECLARE @ayer DATETIME
SET @ayer = DATEADD(day, -1, CAST(GETDATE() AS date))

DECLARE @inicioMesActual DATETIME
SET @inicioMesActual = DATEADD(month, DATEDIFF(month, 0, @ayer), 0)

DECLARE @inicioMesAnterior DATETIME
SET @inicioMesAnterior = DATEADD(M, -1, @inicioMesActual)

DECLARE @inicioMesAnteriorInicio DATETIME
SET @inicioMesAnteriorInicio = DATEADD(month, -1, @inicioMesActual)

DECLARE @hoy DATETIME
SET @hoy = DATEADD(DAY, DATEDIFF(DAY, 0, CURRENT_TIMESTAMP), 0)

-- Obtener importe del mes actual
SELECT 
    u.UEN, 
    COALESCE(a.ImporteActual, 0) AS ImporteActual, 
    COALESCE(p.Objetivo, 0) AS Objetivo
FROM
    (SELECT DISTINCT UEN 
     FROM SCEgreso 
     WHERE UEN IN ('AZCUENAGA', 'PERDRIEL', 'PERDRIEL2', 'SAN JOSE', 'PUENTE OLIVE', 'LAMADRID')) u
LEFT JOIN
    (SELECT 
         UEN, 
         SUM(Importe) AS ImporteActual 
     FROM SCEgreso 
     WHERE FECHASQL >= @inicioMesActual AND FECHASQL < @hoy
     GROUP BY UEN) a
ON u.UEN = a.UEN
LEFT JOIN
    (SELECT 
         UEN, 
         SUM(Importe)*1.1 AS Objetivo 
     FROM SCEgreso 
     WHERE FECHASQL >= @inicioMesAnterior AND FECHASQL < @inicioMesActual
     GROUP BY UEN) p
ON u.UEN = p.UEN
ORDER BY u.UEN;

  ''',db_conex)
ventas_Full = ventas_Full.convert_dtypes()
ventas_Full['UEN'] = ventas_Full['UEN'].str.rstrip()

####### VENTAS XPRESS

ventas_Xpress= pd.read_sql('''
DECLARE @ayer DATETIME
SET @ayer = DATEADD(day, -1, CAST(GETDATE() AS date))

DECLARE @inicioMesActual DATETIME
SET @inicioMesActual = DATEADD(month, DATEDIFF(month, 0, @ayer), 0)

DECLARE @inicioMesAnterior DATETIME
SET @inicioMesAnterior = DATEADD(M, -1, @inicioMesActual)

DECLARE @inicioMesAnteriorInicio DATETIME
SET @inicioMesAnteriorInicio = DATEADD(month, -1, @inicioMesActual)

DECLARE @hoy DATETIME
SET @hoy = DATEADD(DAY, DATEDIFF(DAY, 0, CURRENT_TIMESTAMP), 0)
select UEN,FECHASQL,CANTIDAD,IMPORTE,FECHAMOVIMSQL from SCEgreso 

where FECHASQL >= @inicioMesAnterior
AND FECHASQL < @hoy
and UEN = 'xpress'
order by FECHAMOVIMSQL

  ''',db_conex)
ventas_Xpress = ventas_Xpress.convert_dtypes()
ventas_Xpress['UEN'] = ventas_Xpress['UEN'].str.rstrip()

# Supongamos que tienes el dataframe `df` con las columnas 'importe', 'cantidad', 'fecha' 
ventas_Xpress['datetime'] = pd.to_datetime(ventas_Xpress['FECHAMOVIMSQL'])

# Filtrar datos del mes actual y del mes anterior
now = pd.Timestamp.now()
start_of_current_month = now.replace(day=1)
start_of_previous_month = (start_of_current_month - pd.DateOffset(months=1)).replace(day=1)

df_current_month = ventas_Xpress[(ventas_Xpress['datetime'] >= start_of_current_month) & (ventas_Xpress['datetime'] < now)]
df_previous_month = ventas_Xpress[(ventas_Xpress['datetime'] >= start_of_previous_month) & (ventas_Xpress['datetime'] < start_of_current_month)]

# Función para asignar group_id por intervalo de tiempo
def assign_group_id(df):
    group_id = 0
    df['group_id'] = group_id
    start_time = df['datetime'].iloc[0]
    for i in range(1, len(df)):
        current_time = df['datetime'].iloc[i]
        if (current_time - start_time).total_seconds() > 60:
            group_id += 1
            start_time = current_time
        df.at[i, 'group_id'] = group_id
    return df

# Asignar group_id en ambos dataframes
df_current_month = assign_group_id(df_current_month.sort_values(by='datetime'))
df_previous_month = assign_group_id(df_previous_month.sort_values(by='datetime'))

# Calcular métricas
ventas = df_current_month['IMPORTE'].sum()
clientes = df_current_month['group_id'].nunique()
objetivo_ventas = (df_previous_month['IMPORTE'].sum())*1.1
objetivo_clientes = (df_previous_month['group_id'].nunique())*1.1

# Crear un nuevo dataframe con los resultados
ventas_Xpress = pd.DataFrame({
    'UEN':'XPRESS',
    'Ventas': [ventas],
    'Ventas Con Tiquet': [ventas],
    'Clientes': [clientes],
    'Objetivo Ventas': [objetivo_ventas],
    'ObjetivoImporte': [objetivo_ventas],
    'Objetivo Clientes': [objetivo_clientes]
})

df_combinado = pd.merge(ventas_Full, tiquet_Full, on='UEN', how='left')
# Renombrar columnas para claridad
df_combinado = df_combinado.rename(columns={
    'ImporteActual': 'Ventas',
    'ImporteTiquetActual': 'Ventas Con Tiquet',
    'Objetivo': 'Objetivo Ventas',
    'CantTiquetsActual': 'Clientes',
    'ObjetivoTiquet': 'Objetivo Clientes',
})

df_combinado = pd.concat([df_combinado, ventas_Xpress], axis=0).fillna(0)
# Proyectar columnas de ventas y clientes

df_combinado['Ventas'] = (df_combinado['Ventas'] / dias_transcurridos) * dias_totales_mes
df_combinado['Clientes'] = (df_combinado['Clientes'] / dias_transcurridos) * dias_totales_mes
df_combinado['Ventas Con Tiquet'] = (df_combinado['Ventas Con Tiquet'] / dias_transcurridos) * dias_totales_mes

# Calcular columnas adicionales
df_combinado['Ventas Sin Tiquet'] = df_combinado['Ventas'] - df_combinado['Ventas Con Tiquet']
df_combinado['Consumo Promedio por Cliente'] = df_combinado['Ventas Con Tiquet'] / df_combinado['Clientes']
df_combinado['Objetivo Consumo Promedio'] = df_combinado['ObjetivoImporte'] / df_combinado['Objetivo Clientes']

# Calcular columnas Desvios
df_combinado['Desvio Ventas'] = (df_combinado['Ventas'] - df_combinado['Objetivo Ventas'])/df_combinado['Objetivo Ventas']
df_combinado['Desvio Clientes'] =(df_combinado['Clientes'] - df_combinado['Objetivo Clientes'])/df_combinado['Objetivo Clientes']
df_combinado['Desvio Consumo Promedio'] = (df_combinado['Consumo Promedio por Cliente'] - df_combinado['Objetivo Consumo Promedio'])/df_combinado['Objetivo Consumo Promedio']


# Seleccionar y ordenar columnas finales
df_resultado = df_combinado[['UEN', 'Ventas', 'Objetivo Ventas', 'Desvio Ventas',
                             'Clientes', 'Objetivo Clientes', 'Desvio Clientes', 
                             'Consumo Promedio por Cliente','Objetivo Consumo Promedio','Desvio Consumo Promedio']]

df_resultado = df_resultado.reset_index(drop=True)

from pathlib import Path
import os
import dataframe_image as dfi
from PIL import Image, ImageDraw, ImageFont

def _estiladorVtaTitulo(df, list_Col_Porcentajes, list_Col_num, list_Col_float, titulo):
    """
    Esta función devuelve un dataframe estilizado que debe ser asignado a una variable.
    ARGUMENTOS:
        df: DataFrame que será estilizado.
        list_Col_Porcentajes: Lista de columnas numéricas que se formatearán como porcentaje.
        list_Col_num: Lista de columnas numéricas que se formatearán con cero decimales y separador de miles.
        list_Col_float: Lista de columnas numéricas que se formatearán con dos decimales.
        titulo: Cadena de texto para el título de la tabla.
    """
    resultado = df.style \
        .format("{:,.2%}", subset=list_Col_Porcentajes) \
        .format("{:,.0f}", subset=list_Col_num) \
        .format("{:,.2f}", subset=list_Col_float) \
        .hide(axis=0) \
        .set_caption("<br>" + titulo + "<br>" + (primer_dia_mes.strftime("%d/%m/%y")) + " - " + ((tiempoInicio - pd.to_timedelta(1, "days")).strftime("%d/%m/%y")))\
        .set_properties(subset=list_Col_Porcentajes + list_Col_num + list_Col_float, 
                        **{"text-align": "center", "width": "100px"}) \
        .set_properties(border="2px solid black") \
        .set_table_styles([
            {"selector": "caption",
             "props": [("font-size", "20px"), ("text-align", "center")]},
            {"selector": "th",
             "props": [("text-align", "center"), ("background-color", "black"), ("color", "white")]}
        ]) \
        .apply(lambda x: ["background: black" if x.name == 14 else "" for i in x], axis=1) \
        .apply(lambda x: ["color: white" if x.name == 14 else "" for i in x], axis=1)
    
    # Aplicar colores condicionalmente a los porcentajes
    resultado = resultado.applymap(table_color, subset=list_Col_Porcentajes)

    return resultado

def table_color(val):
    color = 'blue' if val > 0 else 'red'
    return 'color: %s' % color

def df_to_image_with_note(df, ubicacion, nombre, nota):
    dfi.export(df, ubicacion+nombre, max_rows=-1)
    
    img = Image.open(ubicacion + nombre)
    new_img = Image.new('RGB', (img.width, img.height + 60), (255, 255, 255))
    new_img.paste(img, (0, 0))
    
    draw = ImageDraw.Draw(new_img)
    
    # Intentar cargar una fuente de sistema estándar
    try:
        if os.name == 'nt':  # Windows
            font_path = "C:/Windows/Fonts/Arial.ttf"
        elif os.name == 'posix':  # Unix/Linux/MacOS
            font_path = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
        else:
            raise IOError("Ruta de fuente no especificada y el sistema no es compatible")
        
        font_size = 16  # Aumentar el tamaño de la fuente
        font = ImageFont.truetype(font_path, font_size)
    except IOError:
        font = ImageFont.load_default()  # Fuente predeterminada si no se encuentra el archivo de fuente
    
    text_position = (10, img.height + 10)
    draw.text(text_position, nota, font=font, fill="black")
    
    new_img.save(ubicacion + nombre)

# Ejemplo de uso:
percColsPorcentaje = ['Desvio Ventas', 'Desvio Clientes', 'Desvio Consumo Promedio']
list_Col_num = ['Ventas', 'Objetivo Ventas', 'Clientes', 'Objetivo Clientes']
list_Col_float = ['Consumo Promedio por Cliente', 'Objetivo Consumo Promedio']

df_resultado = _estiladorVtaTitulo(df_resultado, percColsPorcentaje, list_Col_num, list_Col_float, "Objetivo Presupuestario Full Proyectado")

ubicacion = str(Path(__file__).parent) + "\\"
nombreN = "Objetivos_Fulls.png"
nota = "**El Consumo Promedio por Cliente se calcula sin tener en cuenta Cigarrillos"

df_to_image_with_note(df_resultado, ubicacion, nombreN, nota)