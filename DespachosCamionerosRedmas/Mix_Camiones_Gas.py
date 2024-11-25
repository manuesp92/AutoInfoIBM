import os
import math
import numpy as np
from PIL import Image
import pandas as pd
from pandas.api.types import CategoricalDtype
import pyodbc #Library to connect to Microsoft SQL Server
import sys
import pathlib
import dataframe_image as dfi
from datetime import datetime
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


mix_prom = pd.read_sql('''

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


SELECT SUM(d.volumen) volumen,d.UEN,SUM(t.objetivo) AS Objetivo,d.CODPRODUCTO
  FROM [Test_Rumaos].[dbo].[H_Despachos] as d left join 
  (SELECT volumen objetivo, UEN, CODPRODUCTO
  FROM [Test_Rumaos].[dbo].[H_Despachos] where  DESP_CAMION = 'Pesado' 
  and FECHASQL >= @inicioMesAnterior
  and FECHASQL < @inicioMesActual
  and UEN in ('perdriel','perdriel2','merc guaymallen','mercado 2','puente olive')) as t 
  on d.UEN = t.UEN
  and d.CODPRODUCTO=t.CODPRODUCTO
  where  DESP_CAMION = 'Pesado' 
  and FECHASQL >= @inicioMesActual
  and FECHASQL < @hoy
  and d.UEN in ('perdriel','perdriel2','merc guaymallen','mercado 2','puente olive')
  and d.CODPRODUCTO in ('GO','EU')
  group by d.UEN,d.CODPRODUCTO
  
  '''      ,db_conex)

mix_prom = mix_prom.convert_dtypes()
mix_prom['UEN']=mix_prom['UEN'].str.strip()
mix_prom['CODPRODUCTO']=mix_prom['CODPRODUCTO'].str.strip()
# Separar los volúmenes y objetivos por CODPRODUCTO
volumen_eu = mix_prom[mix_prom['CODPRODUCTO'] == 'EU'].groupby('UEN')['volumen'].sum()
volumen_go = mix_prom[mix_prom['CODPRODUCTO'] == 'GO'].groupby('UEN')['volumen'].sum()

objetivo_eu = mix_prom[mix_prom['CODPRODUCTO'] == 'EU'].groupby('UEN')['Objetivo'].sum()
objetivo_go = mix_prom[mix_prom['CODPRODUCTO'] == 'GO'].groupby('UEN')['Objetivo'].sum()

# Unir las series de volúmenes y objetivos en un nuevo DataFrame
df_mix = pd.DataFrame({
    'volumen_EU': volumen_eu,
    'volumen_GO': volumen_go,
    'objetivo_EU': objetivo_eu,
    'objetivo_GO': objetivo_go
})

# Calcular el mix actual y el mix objetivo
df_mix['Mix Gasoleos'] = df_mix['volumen_EU'] / (df_mix['volumen_EU'] + df_mix['volumen_GO'])
df_mix['Objetivo'] = (df_mix['objetivo_EU'] / (df_mix['objetivo_EU'] + df_mix['objetivo_GO']))*1.03
df_mix['Desvio']= (df_mix['Mix Gasoleos']-df_mix['Objetivo'])/df_mix['Objetivo']


df_mix=df_mix.reset_index() 

df_mix = df_mix.reindex(columns=['UEN','Mix Gasoleos','Objetivo','Desvio'])


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

df_mix_Estilo = estiladorVtaTitulo(
    df_mix
    ,"Desvio"
    ,["Mix Gasoleos", "Objetivo"]
    , "Mix Gasoleos Camiones acumulados al: "
)

ubicacion = str(pathlib.Path(__file__).parent)+"\\"
nombrePen = "Mix_Gasoleos_Camiones.png"
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

df_to_image(df_mix_Estilo, ubicacion, nombrePen)
