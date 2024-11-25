import os
import math
import numpy as np
#from PIL import Image
import pandas as pd
from pandas.api.types import CategoricalDtype
import pyodbc #Library to connect to Microsoft SQL Server
import sys
import pathlib
from datetime import datetime
from datetime import timedelta
from datetime import date
from datetime import timedelta,datetime
import dataframe_image as dfi
from calendar import monthrange
import logging
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


despachos_YER = pd.read_sql('''
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


  SELECT 
    (SELECT COUNT(DISTINCT ID) 
     FROM [Test_Rumaos].[dbo].[H_VentasYER] 
     WHERE FECHASQL >= @inicioMesActual AND FECHASQL < @hoy
       AND CODPRODUCTO IN ('GO', 'EU', 'NS', 'NU')
         and NOMBRECLIENTE <> '') AS 'Despachos YER',
       
    (SELECT COUNT(DISTINCT ID) * 1.05
     FROM [Test_Rumaos].[dbo].H_VentasYER 
     WHERE FECHASQL >= @inicioMesAnterior AND FECHASQL < CAST(DATEADD(MONTH, -1, GETDATE()) AS DATE)
       AND CODPRODUCTO IN ('GO', 'EU', 'NS', 'NU')
         and NOMBRECLIENTE <> '') AS 'Objetivo'
  
  '''      ,db_conex)
despachos_YER = despachos_YER.convert_dtypes()

despachos_CtaCte = pd.read_sql('''

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


SELECT 
    (SELECT COUNT(*) 
     FROM [Test_Rumaos].[dbo].H_VtaCtaCte 
     WHERE FECHASQL >= @inicioMesActual AND FECHASQL < @hoy
       AND CODPRODUCTO IN ('GO', 'EU', 'NS', 'NU')
       AND NROCLIENTE > 100000 
       AND NROCLIENTE < 500000) AS 'Despachos CtaCte',
       
    (SELECT COUNT(*) *1.05
     FROM [Test_Rumaos].[dbo].H_VtaCtaCte 
     WHERE FECHASQL >= @inicioMesAnterior AND FECHASQL < CAST(DATEADD(MONTH, -1, GETDATE()) AS DATE)
       AND CODPRODUCTO IN ('GO', 'EU', 'NS', 'NU')
       AND NROCLIENTE > 100000 
       AND NROCLIENTE < 500000) AS 'Objetivo'
  
  '''      ,db_conex)

despachos_CtaCte = despachos_CtaCte.convert_dtypes()


despachos_CamionesContado = pd.read_sql('''

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


SELECT 
    ( SELECT SUM(DESPACHOS) FROM (

    SELECT  distinct ID,CANT_DESPACHOS DESPACHOS,D.UEN, D.FECHASQL, D.CODPRODUCTO, D.VOLUMEN, D.IMPORTE, D.TURNO,
			IIF(D.TARJETA = '', 'contado', D.TARJETA) AS TARJETA,R.NROCLIENTE,F.NROCLIPRO,d.PTOVTA,d.NROCOMP,d.TIPOCOMP
  FROM (select  ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS ID,* from [Test_Rumaos].[dbo].[H_Despachos]) as d 
  LEFT JOIN H_FacRemDet R
		ON D.UEN = R.UEN
		AND cast(D.TURNO as varchar(50)) = R.TURNO
		AND D.TIPOCOMP = 'REM'
		AND D.PTOVTA = R.PTOVTA
		AND D.NROCOMP = R.NROREMITO
		AND D.VOLUMEN = R.CANTIDAD
	LEFT JOIN H_FAC001 F
		ON D.UEN = F.UEN
		AND D.TIPOCOMP = F.TIPOCOMP
		AND D.PTOVTA = F.PTOVTA
		AND D.NROCOMP = F.NROCOMP
		AND D.LETRACOMP = F.LETRACOMP
		AND cast(D.TURNO as varchar(50)) = F.TURNO
		WHERE D.DESP_CAMION = 'Pesado' 
		AND D.DESCRIPCION NOT LIKE '%ypf%' 
		AND ((R.NROCLIENTE < 100000 OR F.NROCLIPRO < 100000) OR (R.NROCLIENTE > 500000 OR F.NROCLIPRO > 500000))
		AND D.FECHASQL >= '2023-01-01'

UNION ALL

SELECT  distinct ID,CANT_DESPACHOS DESPACHOS,D.UEN, D.FECHASQL, D.CODPRODUCTO, D.VOLUMEN, D.IMPORTE, D.TURNO,
			IIF(D.TARJETA = '', 'contado', D.TARJETA) AS TARJETA,0 AS NROCLIENTE,0 AS NROCLIPRO,d.PTOVTA,d.NROCOMP,d.TIPOCOMP
  FROM (select  1000000+ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS ID,* from [Test_Rumaos].[dbo].[H_Despachos]) as d 
  WHERE
	 D.DESP_CAMION = 'Pesado' 
	AND D.DESCRIPCION NOT LIKE '%ypf%' 
	AND D.TIPOCOMP = ''
	AND D.NROCOMP = 0
	AND D.FECHASQL >= '2023-01-01'         

    ) AS T
     WHERE FECHASQL >= @inicioMesActual AND FECHASQL < @hoy
       AND CODPRODUCTO IN ('GO', 'EU', 'NS', 'NU')
       ) AS 'Despachos Camiones Contado',
    
    
    (SELECT SUM(DESPACHOS) *1.05 FROM (
    
    SELECT  distinct ID,CANT_DESPACHOS DESPACHOS,D.UEN, D.FECHASQL, D.CODPRODUCTO, D.VOLUMEN, D.IMPORTE, D.TURNO,
			IIF(D.TARJETA = '', 'contado', D.TARJETA) AS TARJETA,R.NROCLIENTE,F.NROCLIPRO,d.PTOVTA,d.NROCOMP,d.TIPOCOMP
  FROM (select  ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS ID,* from [Test_Rumaos].[dbo].[H_Despachos]) as d 
  LEFT JOIN H_FacRemDet R
		ON D.UEN = R.UEN
		AND cast(D.TURNO as varchar(50)) = R.TURNO
		AND D.TIPOCOMP = 'REM'
		AND D.PTOVTA = R.PTOVTA
		AND D.NROCOMP = R.NROREMITO
		AND D.VOLUMEN = R.CANTIDAD
	LEFT JOIN H_FAC001 F
		ON D.UEN = F.UEN
		AND D.TIPOCOMP = F.TIPOCOMP
		AND D.PTOVTA = F.PTOVTA
		AND D.NROCOMP = F.NROCOMP
		AND D.LETRACOMP = F.LETRACOMP
		AND cast(D.TURNO as varchar(50)) = F.TURNO
		WHERE D.DESP_CAMION = 'Pesado' 
		AND D.DESCRIPCION NOT LIKE '%ypf%' 
		AND ((R.NROCLIENTE < 100000 OR F.NROCLIPRO < 100000) OR (R.NROCLIENTE > 500000 OR F.NROCLIPRO > 500000))
		AND D.FECHASQL >= '2023-01-01'

UNION ALL

SELECT  distinct ID,CANT_DESPACHOS DESPACHOS,D.UEN, D.FECHASQL, D.CODPRODUCTO, D.VOLUMEN, D.IMPORTE, D.TURNO,
			IIF(D.TARJETA = '', 'contado', D.TARJETA) AS TARJETA,0 AS NROCLIENTE,0 AS NROCLIPRO,d.PTOVTA,d.NROCOMP,d.TIPOCOMP
  FROM (select  1000000+ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS ID,* from [Test_Rumaos].[dbo].[H_Despachos]) as d 
  WHERE
	 D.DESP_CAMION = 'Pesado' 
	AND D.DESCRIPCION NOT LIKE '%ypf%' 
	AND D.TIPOCOMP = ''
	AND D.NROCOMP = 0
	AND D.FECHASQL >= '2023-01-01'         
                              
    ) AS T
     WHERE FECHASQL >= @inicioMesAnterior AND FECHASQL < CAST(DATEADD(MONTH, -1, GETDATE()) AS DATE)
       AND CODPRODUCTO IN ('GO', 'EU', 'NS', 'NU')
       ) AS 'Objetivo'
  
  '''      ,db_conex)

despachos_CamionesContado= despachos_CamionesContado.convert_dtypes()


# Calcular el mix actual y el mix objetivo

despachos_CtaCte['Desvio']= (despachos_CtaCte['Despachos CtaCte']-despachos_CtaCte['Objetivo'])/despachos_CtaCte['Objetivo']
despachos_YER['Desvio']= (despachos_YER['Despachos YER']-despachos_YER['Objetivo'])/despachos_YER['Objetivo']
despachos_CamionesContado['Desvio']= (despachos_CamionesContado['Despachos Camiones Contado']-despachos_CamionesContado['Objetivo'])/despachos_CamionesContado['Objetivo']

# Crear una nueva columna 'Despachos' en cada DataFrame
despachos_YER['Despachos'] = 'Despachos YER'
despachos_CtaCte['Despachos'] = 'Despachos CtaCte'
despachos_CamionesContado['Despachos'] = 'Despachos Camiones Contado'

# Renombrar las columnas para que tengan el mismo formato en los DataFrames
despachos_YER = despachos_YER.rename(columns={'Despachos YER': 'Mes Actual', 'Objetivo': 'Objetivo', 'Desvio': 'Desvío'})
despachos_CtaCte = despachos_CtaCte.rename(columns={'Despachos CtaCte': 'Mes Actual', 'Objetivo': 'Objetivo', 'Desvio': 'Desvío'})
despachos_CamionesContado = despachos_CamionesContado.rename(columns={'Despachos Camiones Contado': 'Mes Actual', 'Objetivo': 'Objetivo', 'Desvio': 'Desvío'})

# Unir los DataFrames usando pd.concat
despachos_final = pd.concat([despachos_YER[['Despachos', 'Mes Actual', 'Objetivo', 'Desvío']], 
                            despachos_CtaCte[['Despachos', 'Mes Actual', 'Objetivo', 'Desvío']],
                            despachos_CamionesContado[['Despachos', 'Mes Actual', 'Objetivo', 'Desvío']]],
                           ignore_index=True)



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
        .format("{0:,.0f}", subset=listaColNumericas) \
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
        .applymap(lambda x: 'color: blue' if x >= 0 else 'color: #FF0000', subset=pd.IndexSlice[:, ['Desvío']]) \
        .apply(lambda x: ["background: black" if x.name == "colTOTAL" 
            else "" for i in x]
            , axis=1) \
        .apply(lambda x: ["color: white" if x.name == "colTOTAL" 
            else "" for i in x]
            , axis=1)
    return resultado

df_Despachos_Camiones_estilado = estiladorVtaTitulo(
    despachos_final
    ,"Desvío"
    ,["Mes Actual", "Objetivo"]
    , "Cantidad de Despachos Camiones al: "
)

ubicacion = str(pathlib.Path(__file__).parent)+"\\"
nombrePen = "Cant_Clientes_Camiones.png"

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

df_to_image(df_Despachos_Camiones_estilado, ubicacion, nombrePen)