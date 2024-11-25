import os
import math
import numpy as np
from PIL import Image
import pandas as pd
from pandas.api.types import CategoricalDtype
import pyodbc #Library to connect to Microsoft SQL Server
import sys
import pathlib
from datetime import datetime
import dataframe_image as dfi
from datetime import timedelta
from datetime import date
from datetime import timedelta,datetime
from calendar import monthrange
server = "192.168.200.44\cloud"
database = "Test_Rumaos"
username = "mmagistretti" 
password = "R3dmer0s#r"

login = [server,database,username,password]

tiempoInicio = pd.to_datetime("today")
#########
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


volumen_Total = pd.read_sql('''
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

    -- Inicio del mes actual del año anterior
    DECLARE @inicioMesActualAñoAnterior DATETIME
    SET @inicioMesActualAñoAnterior = DATEADD(year, -1, @inicioMesActual)

    -- Fin del mes actual del año anterior
    DECLARE @finMesActualAñoAnterior DATETIME
    SET @finMesActualAñoAnterior = DATEADD(DAY, -1, EOMONTH(@inicioMesActualAñoAnterior))

    DECLARE @hoyAñoAnterior DATE
    SET @hoyAñoAnterior = CAST(DATEADD(year, -1, GETDATE()) AS DATE)


 SELECT Sum(d.VOLUMEN) Volumen,d.UEN,sum(Objetivo) as Objetivo
     FROM [Test_Rumaos].[dbo].[H_Despachos] as d left join
	 (SELECT VOLUMEN Objetivo,UEN
     FROM [Test_Rumaos].[dbo].[H_Despachos] 
     WHERE FECHASQL >= @inicioMesAnterior AND FECHASQL < CAST(DATEADD(MONTH, -1, GETDATE()) AS DATE)
       AND CODPRODUCTO IN ('GO', 'EU', 'NS', 'NU')
         and DESP_CAMION = 'Pesado'
		 ) AS t
		 on d.UEN = t.uen
     WHERE FECHASQL >= @inicioMesActual AND FECHASQL < @hoy
       AND CODPRODUCTO IN ('GO', 'EU', 'NS', 'NU')
       and d.UEN in ('perdriel','perdriel2','merc guaymallen','mercado 2','puente olive')
         and DESP_CAMION = 'Pesado'
       group by d.UEN
  
  '''      ,db_conex)
volumen_Total = volumen_Total.convert_dtypes()
volumen_Total = volumen_Total.convert_dtypes()
volumen_Total['UEN']=volumen_Total['UEN'].str.strip()

volumen_RedMas = pd.read_sql('''

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

    -- Inicio del mes actual del año anterior
    DECLARE @inicioMesActualAñoAnterior DATETIME
    SET @inicioMesActualAñoAnterior = DATEADD(year, -1, @inicioMesActual)

    -- Fin del mes actual del año anterior
    DECLARE @finMesActualAñoAnterior DATETIME
    SET @finMesActualAñoAnterior = DATEADD(DAY, -1, EOMONTH(@inicioMesActualAñoAnterior))

    DECLARE @hoyAñoAnterior DATE
    SET @hoyAñoAnterior = CAST(DATEADD(year, -1, GETDATE()) AS DATE)


 SELECT Sum(d.VOLUMEN) VolumenRM,d.UEN,sum(Objetivo)*1.05 as ObjetivoRM
     FROM [Test_Rumaos].[dbo].[H_Despachos] as d left join
	 (SELECT VOLUMEN Objetivo,UEN
     FROM [Test_Rumaos].[dbo].[H_Despachos] 
     WHERE FECHASQL >= @inicioMesAnterior AND FECHASQL < CAST(DATEADD(MONTH, -1, GETDATE()) AS DATE)
       AND CODPRODUCTO IN ('GO', 'EU', 'NS', 'NU')
         and DESP_CAMION = 'Pesado'
		and tarjeta like 'I%' ) AS t
		 on d.UEN = t.uen
     WHERE FECHASQL >= @inicioMesActual AND FECHASQL < @hoy
       AND CODPRODUCTO IN ('GO', 'EU', 'NS', 'NU')
       and DESP_CAMION = 'Pesado'
       and d.UEN in ('perdriel','perdriel2','merc guaymallen','mercado 2','puente olive')
		and tarjeta like 'I%'
       group by d.UEN
  
  '''      ,db_conex)

volumen_RedMas = volumen_RedMas.convert_dtypes()
volumen_RedMas = volumen_RedMas.convert_dtypes()
volumen_RedMas['UEN']=volumen_RedMas['UEN'].str.strip()


# Calcular el mix actual y el mix objetivo
penetracion = volumen_Total.merge(volumen_RedMas,on='UEN',how='outer')
penetracion['Penetracion'] = penetracion['VolumenRM']/penetracion['Volumen']
penetracion['Objetivo'] = penetracion['ObjetivoRM']/penetracion['Objetivo']
penetracion['Desvio']= (penetracion['Penetracion']-penetracion['Objetivo'])/penetracion['Objetivo']
penetracion = penetracion.reindex(columns=['UEN','Penetracion','Objetivo','Desvio'])


def estiladorVtaTitulo(df,listaColPorc,listaColNumericas,titulo):
    """
This function will return a styled dataframe that must be assign to a variable.
ARGS:
    df: Dataframe that will be styled.
    listaColNumericas: List of numeric columns that will be formatted with
    zero decimals and thousand separator.
    titulo: String for the table caption.
    """
    resultado = df.style \
        .format("{0:,.2%}", subset=listaColNumericas) \
        .format("{0:,.2%}", subset=listaColPorc) \
        .hide(axis=0) \
        .set_caption(titulo
            +"\n"
            +((tiempoInicio-pd.to_timedelta(1,"days")).strftime("%d/%m/%y"))
        ) \
        .set_properties(subset=listaColNumericas
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
        .applymap(lambda x: 'color: blue' if x >= 0 else 'color: #FF0000', subset=pd.IndexSlice[:, ['Desvio']]) \
        .apply(lambda x: ["background: black" if x.name == "colTOTAL" 
            else "" for i in x]
            , axis=1) \
        .apply(lambda x: ["color: white" if x.name == "colTOTAL" 
            else "" for i in x]
            , axis=1)
    return resultado

df_penetracion_estilado = estiladorVtaTitulo(
    penetracion
    ,["Desvio","Penetracion", "Objetivo"]
    ,[]
    , "Penetracion RedMas Camiones acumulados al: "
)

ubicacion = str(pathlib.Path(__file__).parent)+"\\"
nombrePen = "objetivo_Red_Mas_Camiones.png"
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

df_to_image(df_penetracion_estilado, ubicacion, nombrePen)