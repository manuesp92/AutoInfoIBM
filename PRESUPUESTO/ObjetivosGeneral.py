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

server = "192.168.200.44\cloud"
database = "Test_Rumaos"
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

  ###PRESUPUESTO  
sheet_id='1ofJ38O2-dLfw7TZOxeTDYUzlWgl6hN2iMLZCMscTgGo'
hoja='OBJETIVOS'
gsheet_url_presupuesto = "https://docs.google.com/spreadsheets/d/{}/gviz/tq?tqx=out:csv&sheet={}".format(sheet_id, hoja)
df_presupuesto=pd.read_csv(gsheet_url_presupuesto)
df_presupuesto = df_presupuesto.convert_dtypes()
df_presupuesto['FECHA'] = pd.to_datetime(df_presupuesto['FECHA'], format='%d/%m/%Y')
df_presupuesto= df_presupuesto.loc[(df_presupuesto["FECHA"] <= ayer.strftime('%Y-%m-%d'))& (df_presupuesto["FECHA"] >= primer_dia_mes.strftime('%Y-%m-%d')),:]

ventas_Playa = pd.read_sql('''
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
select  SUM(CANT_DESPACHOS) CANT_DESPACHOS,UEN,FECHASQL,CODPRODUCTO,SUM(VOLUMEN) VOLUMEN from H_Despachos 
WHERE FECHASQL >= @inicioMesActual
        AND FECHASQL < @hoy
GROUP BY UEN,FECHASQL,CODPRODUCTO ORDER BY FECHASQL DESC

  ''',db_conex)
ventas_Playa = ventas_Playa.convert_dtypes()
ventas_Playa['CODPRODUCTO'] = ventas_Playa['CODPRODUCTO'].str.rstrip()
ventas_Playa['UEN'] = ventas_Playa['UEN'].str.rstrip()

df_presupuesto = df_presupuesto.reindex(columns = ['UEN','FECHA','Nafta Super','Mix Infinia Nafta','Ultra diesel','Mix Infinia Gasoil','OBJETIVO NAFTAS','OBJETIVO GASOLEOS','OBJETIVO GNC','Despachos Gasoil','Despachos Naftas','Despachos GNC'])
df_presupuesto.dtypes
# Convertir las columnas especificadas a int

columnas_a_convertir = ['OBJETIVO NAFTAS','OBJETIVO GASOLEOS','OBJETIVO GNC','Despachos Gasoil','Despachos Naftas','Despachos GNC']

# Función para eliminar comas, puntos y convertir a int
def convertir_a_int(columna):
    return columna.str.replace(',', '').astype(float).astype(int)

# Aplicar la función a las columnas seleccionadas
df_presupuesto[columnas_a_convertir] = df_presupuesto[columnas_a_convertir].apply(convertir_a_int)

fecha_actual = ventas_Playa['FECHASQL'].max()
dias_transcurridos = fecha_actual.day

# Calculamos el número total de días del mes actual
dias_totales_mes = fecha_actual.days_in_month if hasattr(fecha_actual, 'days_in_month') else pd.Period(fecha_actual.strftime('%Y-%m')).days_in_month


# Filtrar productos líquidos (Nafta e Infinia) y GNC
productos_liquidos_naftas = ['NS', 'NU']
productos_liquidos_gasoil = ['GO', 'EU']
productos_gnc = ['GNC']


# Filtrar y agrupar datos por UEN
def filtrar_y_agrupar(df, productos):
    df_filtrado = df[df['CODPRODUCTO'].isin(productos)]
    df_agrupado = df_filtrado.drop(columns=['FECHASQL', 'CODPRODUCTO']).groupby('UEN').sum().reset_index()
    return df_agrupado


# DataFrame de líquidos
df_liquidos_naftas = filtrar_y_agrupar(ventas_Playa, productos_liquidos_naftas)
df_gnc = filtrar_y_agrupar(ventas_Playa, productos_gnc)
df_liquidos_gasoil = filtrar_y_agrupar(ventas_Playa, productos_liquidos_gasoil)

# Función para calcular métricas con proyección
def calcular_metricas_proyectadas(df, df_objetivos, tipo_objetivo, tipo_despachos):
    df = df.merge(df_objetivos, left_on='UEN', right_on='UEN', how='inner')
    df['VOLUMEN_PROY'] = (df['VOLUMEN'] / dias_transcurridos) * dias_totales_mes
    df['DESPACHOS_PROY'] = (df['CANT_DESPACHOS'] / dias_transcurridos) * dias_totales_mes
    df['VOL_DESVIO'] = (df['VOLUMEN_PROY'] - df[tipo_objetivo])/df[tipo_objetivo]
    df['VOL_PROM_DESPACHO'] = df['VOLUMEN_PROY'] / df['DESPACHOS_PROY']
    df['OBJ_VOL_PROM_DESPACHO'] = df[tipo_objetivo] / df[tipo_despachos]
    df['DESPACHO_DESVIO'] = (df['DESPACHOS_PROY'] - df[tipo_despachos])/df[tipo_despachos]
    df['DESVIO_VOL_PROM_DESPACHO'] = (df['VOL_PROM_DESPACHO'] - df['OBJ_VOL_PROM_DESPACHO'])/df['OBJ_VOL_PROM_DESPACHO']
    # Selecciona y renombra las columnas de interés
    return df[['UEN', 'VOLUMEN_PROY', tipo_objetivo, 'VOL_DESVIO', 'DESPACHOS_PROY', tipo_despachos, 'DESPACHO_DESVIO', 'VOL_PROM_DESPACHO', 'OBJ_VOL_PROM_DESPACHO','DESVIO_VOL_PROM_DESPACHO']]


# Aplicar la función para líquidos y GNC
df_liquidos_proy_naftas = calcular_metricas_proyectadas(df_liquidos_naftas, df_presupuesto, 'OBJETIVO NAFTAS', 'Despachos Naftas')
df_liquidos_proy_gasoil = calcular_metricas_proyectadas(df_liquidos_gasoil, df_presupuesto, 'OBJETIVO GASOLEOS', 'Despachos Gasoil')
df_gnc_proy = calcular_metricas_proyectadas(df_gnc, df_presupuesto, 'OBJETIVO GNC', 'Despachos GNC')

def agregar_columnas_totales(df, vol_col, desp_col):
    # Sumar columnas numéricas para el total
    df_totales = df.sum(numeric_only=True)
    df_totales['UEN'] = 'Total'
    
    # Calcular promedios ponderados para columnas de porcentaje
    if 'VOL_DESVIO' in df.columns:
        df_totales['VOL_DESVIO'] = (df['VOLUMEN_PROY'] - df[vol_col]).sum() / df[vol_col].sum()
    if 'DESPACHO_DESVIO' in df.columns:
        df_totales['DESPACHO_DESVIO'] = (df['DESPACHOS_PROY'] - df[desp_col]).sum() / df[desp_col].sum()
    if 'DESVIO_VOL_PROM_DESPACHO' in df.columns:
        df_totales['DESVIO_VOL_PROM_DESPACHO'] = (df['VOL_PROM_DESPACHO'] - df['OBJ_VOL_PROM_DESPACHO']).sum() / df['OBJ_VOL_PROM_DESPACHO'].sum()

    # Añadir la fila de totales al DataFrame
    df_totales = pd.DataFrame(df_totales).transpose()
    df = pd.concat([df, df_totales], ignore_index=True)
    
    return df

# Aplicar la función a cada DataFrame con sus respectivas columnas de volumen y despachos
#df_liquidos_proy_naftas_totales = agregar_columnas_totales(df_liquidos_proy_naftas, 'OBJETIVO NAFTAS', 'Despachos Naftas')
#df_liquidos_proy_gasoil_totales = agregar_columnas_totales(df_liquidos_proy_gasoil, 'OBJETIVO GASOLEOS', 'Despachos Gasoil')
#df_gnc_proy_totales = agregar_columnas_totales(df_gnc_proy, 'OBJETIVO GNC', 'Despachos GNC')

df_liquidos_proy_gasoil = df_liquidos_proy_gasoil.rename(columns= {'VOLUMEN_PROY':'Ventas','OBJETIVO GASOLEOS':'Objetivo','VOL_DESVIO':'Desvio Ventas','DESPACHOS_PROY':'Clientes','Despachos Gasoil':'Objetivo Clientes','DESPACHO_DESVIO':'Desvio Clientes','VOL_PROM_DESPACHO':'Consumo Promedio por Cliente','OBJ_VOL_PROM_DESPACHO':'Objetivo Consumo Promedio','DESVIO_VOL_PROM_DESPACHO':'Desvio Consumo Promedio por Cliente'})

df_liquidos_proy_naftas = df_liquidos_proy_naftas.rename(columns= {'VOLUMEN_PROY':'Ventas','OBJETIVO NAFTAS':'Objetivo','VOL_DESVIO':'Desvio Ventas','DESPACHOS_PROY':'Clientes','Despachos Naftas':'Objetivo Clientes','DESPACHO_DESVIO':'Desvio Clientes','VOL_PROM_DESPACHO':'Consumo Promedio por Cliente','OBJ_VOL_PROM_DESPACHO':'Objetivo Consumo Promedio','DESVIO_VOL_PROM_DESPACHO':'Desvio Consumo Promedio por Cliente'})

df_gnc_proy = df_gnc_proy.rename(columns= {'VOLUMEN_PROY':'Ventas','OBJETIVO GNC':'Objetivo','VOL_DESVIO':'Desvio Ventas','DESPACHOS_PROY':'Clientes','Despachos GNC':'Objetivo Clientes','DESPACHO_DESVIO':'Desvio Clientes','VOL_PROM_DESPACHO':'Consumo Promedio por Cliente','OBJ_VOL_PROM_DESPACHO':'Objetivo Consumo Promedio','DESVIO_VOL_PROM_DESPACHO':'Desvio Consumo Promedio por Cliente'})

def _estiladorVtaTitulo(df, list_Col_Porcentajes,list_Col_num,list_Col_float, titulo):
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
        .format("{:,.2%}", subset=list_Col_Porcentajes)\
        .format("{:,.0f}", subset=list_Col_num)\
        .format("{:,.2f}", subset=list_Col_float)\
        .hide(axis=0) \
        .set_caption(
            titulo
            + "<br>"
            + ((primer_dia_mes).strftime("%d/%m/%y"))
            + " - "
            + ((tiempoInicio-pd.to_timedelta(1,"days")).strftime("%d/%m/%y"))
            + "<br>") \
        .set_properties(subset= list_Col_Porcentajes+list_Col_num+list_Col_float, **{"text-align": "center", "width": "100px"}) \
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
        .apply(lambda x: ["background: black" if x.name == 14 
            else "" for i in x]
            , axis=1) \
        .apply(lambda x: ["color: white" if x.name == 14 
            else "" for i in x]
            , axis=1)
    evitarTotales = df.index.get_level_values(0)
    
    #subset_columns = pd.IndexSlice[evitarTotales[:-0], list_Col_Porcentajes]

    resultado= resultado.applymap(table_color,subset=list_Col_Porcentajes)

    return resultado

def table_color(val):
    """
    Takes a scalar and returns a string with
    the css property `'color: red'` for less than 60 marks, green otherwise.
    """
    color = 'blue' if val > 0 else 'red'
    return 'color: % s' % color


percColsPorcentaje = ['Desvio Ventas','Desvio Clientes','Desvio Consumo Promedio por Cliente']
list_Col_num = ['Ventas','Objetivo','Clientes','Objetivo Clientes']
list_Col_float = ['Consumo Promedio por Cliente','Objetivo Consumo Promedio']

df_liquidos_proy_gasoil = _estiladorVtaTitulo(df_liquidos_proy_gasoil,percColsPorcentaje,list_Col_num,list_Col_float,"Objetivo Presupuestario Gasoil Proyectado")
df_liquidos_proy_naftas = _estiladorVtaTitulo(df_liquidos_proy_naftas,percColsPorcentaje,list_Col_num,list_Col_float,"Objetivo Presupuestario Naftas Proyectado")
df_gnc_proy = _estiladorVtaTitulo(df_gnc_proy,percColsPorcentaje,list_Col_num,list_Col_float,"Objetivo Presupuestario GNC Proyectado")

### APLICO EL FORMATO A LA TABLA
ubicacion = str(pathlib.Path(__file__).parent) + "\\"
nombreN = "Objetivos_Naftas.png"
nombreM = "Objetivos_Gasoil.png"
nombreMG = "Objetivos_GNC.png"


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
        dfi.export(df, ubicacion+nombre,max_rows=-1)
    else:
        dfi.export(df, ubicacion+nombre,max_rows=-1)

df_to_image(df_liquidos_proy_gasoil, ubicacion, nombreM)
df_to_image(df_liquidos_proy_naftas, ubicacion, nombreN)
df_to_image(df_gnc_proy, ubicacion, nombreMG)
