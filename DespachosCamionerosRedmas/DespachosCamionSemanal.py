###################################
#
#     INFORME Primer Despacho por Activación
#             (CAMIONEROS)
#               21/09/21
###################################

import mysql.connector
from mysql.connector import Error
import pyodbc
from DatosLogin import login, loginMySQL
import pathlib
import sys
from datetime import datetime
from datetime import timedelta
sys.path.insert(0,str(pathlib.Path(__file__).parent.parent))
import pandas as pd

import os
import dataframe_image as dfi

import logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        , level=logging.INFO
)
logger = logging.getLogger(__name__)


tiempoInicio = pd.to_datetime("today")


def conectorMSSQL(datos):
    """
    This function will create a connection to a Microsoft SQL Server, should 
    be provided with a list with the following strings in this exact order:
        datos=["server","database","username","password"]
    """
    try:
        conexMSSQL = pyodbc.connect(
            "DRIVER={ODBC Driver 17 for SQL Server};\
            SERVER="+datos[0]+";\
            DATABASE="+datos[1]+";\
            UID="+datos[2]+";\
            PWD="+ datos[3]
        )
        logger.info(
            "\nConnected to server: "
            + datos[0]
            + "\nDatabase: " 
            + datos[1]
        )
        
        return conexMSSQL

    except Exception as e:
        listaErrores = e.args[1].split(".")
        logger.error("\nOcurrió un error al conectar a SQL Server: ")
        for i in listaErrores:
            logger.error(i)
        

# Create database connections

conexMSSQL = conectorMSSQL(login)


# Extracting all the cards numbers that had been activated in the last 92 days 
# and belong to truckers
'''df_tarj_camion = pd.read_sql("""
    SELECT 
        users.tarjeta as 'TARJETA'      
    FROM red_mas_prod.users
    where users.tarjeta like 'ig%'
        #and users.category = '17'
        and register_app_at >= cast(date_add(now(), INTERVAL -92 DAY) as date)
""", conexMySQL)
'''
#df_tarj_camion = df_tarj_camion.convert_dtypes()
#df_tarj_camion["TARJETA"] = df_tarj_camion["TARJETA"].str.strip()


# Consultamos todas las tarjetas, que ademas de cumplir con las condiciones de la consulta, tienen como fecha de alta una fecha igual o mayor a 92
# días antes de hoy y luego filtramos aquellas que hayan sido utilizadas o que hayan consumido una semana para atrás.
# 7 días antes de hoy y pertenecen a determinadas UEN
df_1era_carga = pd.read_sql("""
    SELECT
        D.TARJETA
        ,D.UEN as UEN
        , MIN(FECHADESPSQL) as 'F.DESPACHO'
    FROM [Rumaos].[dbo].[VIEW_DESPAPRO_FILTRADA] as D
        join GDM_SRV.dbo.Cliente as GDM_C on D.TARJETA = GDM_C.TARJETA
    where D.TARJETA like 'ig%'
        and D.UEN IN (
            'MERC GUAYMALLEN'
            ,'MERCADO 2'
            ,'PERDRIEL'
            ,'PERDRIEL2'
            ,'PUENTE OLIVE'
        )
        and GDM_C.Categoria = '17'
        and GDM_C.FechaAlta >= DATEADD(day, -92, CAST(GETDATE() AS date))
    GROUP BY D.TARJETA, D.UEN
    HAVING (
	CAST(MIN(FECHADESPSQL) AS date) BETWEEN  DATEADD(day, -7, CAST(GETDATE() AS date)) AND DATEADD(day, -1, CAST(GETDATE() AS date))
	) ORDER BY UEN
""", conexMSSQL)

df_1era_carga = df_1era_carga.convert_dtypes()
df_1era_carga["UEN"] = df_1era_carga["UEN"].str.strip()
df_1era_carga["TARJETA"] = df_1era_carga["TARJETA"].str.strip()


# Inner merge of "df_1era_carga" and "df_tarj_camion" to get the truckers cards 
# activated in the last 92 days and used for the first time yesterday
df_1era_carga_camion = df_1era_carga

# Using a pivot table to count TARJETA per UEN
pivot_1era_carga_camion = pd.pivot_table(df_1era_carga_camion
    , values="TARJETA"
    , index="UEN"
    , aggfunc="count"
)


pivot_1era_carga_camion = pivot_1era_carga_camion.reset_index()
pivot_1era_carga_camion.rename(columns={"TARJETA":"ACTIVACIONES"},inplace=True)
pivot_1era_carga_camion = pivot_1era_carga_camion.convert_dtypes()

# Counting all the truckers purchases, of yesterday, that dont have an account
df_despachos = pd.read_sql("""
    SELECT
    a.UEN, count(distinct a.DESPACHOS) AS 'DESPACHOS'
FROM
    (SELECT
        D.UEN, D.ID AS 'DESPACHOS'
    FROM
        [Rumaos].[dbo].[VIEW_DESPAPRO_FILTRADA] AS D
    INNER JOIN
        Rumaos.dbo.Fac001 AS F
    ON
        D.TIPOCOMP = F.TIPOCOMP
        AND D.LETRACOMP = F.LETRACOMP
        AND D.PTOVTA = F.PTOVTA
        AND D.NROCOMP = F.NROCOMP
    WHERE 
        D.CODPRODUCTO NOT LIKE 'GNC'
        AND D.UEN IN ('MERC GUAYMALLEN', 'MERCADO 2', 'PUENTE OLIVE')
        AND D.VOLUMEN >= '80'
        AND D.FECHASQL BETWEEN DATEADD(day, -7, CAST(GETDATE() AS date)) AND DATEADD(day, -1, CAST(GETDATE() AS date))
    
    UNION

    SELECT
        D.UEN, D.ID AS 'DESPACHOS'
    FROM
        [Rumaos].[dbo].[VIEW_DESPAPRO_FILTRADA] AS D
    INNER JOIN
        Rumaos.dbo.FacRemDet AS F
    ON 
        D.PTOVTA = F.PTOVTA
        AND D.NROCOMP = F.NROREMITO
    WHERE 
        D.CODPRODUCTO NOT LIKE 'GNC'
        AND D.UEN IN ('MERC GUAYMALLEN', 'MERCADO 2', 'PUENTE OLIVE')
        AND D.VOLUMEN >= '80'
        AND D.FECHASQL BETWEEN DATEADD(day, -7, CAST(GETDATE() AS date)) AND DATEADD(day, -1, CAST(GETDATE() AS date))
    ) AS a
GROUP BY
    a.UEN

UNION ALL

SELECT
    d.UEN, COUNT(DISTINCT d.ID) AS 'DESPACHOS'
FROM Despapro AS d
WHERE
    d.UEN IN ('PERDRIEL', 'PERDRIEL2')
    AND d.CODPRODUCTO IN ('go', 'eu')
    AND d.aforador NOT LIKE '%[a-zA-Z]%'
    AND d.FECHASQL BETWEEN DATEADD(day, -7, CAST(GETDATE() AS date)) AND DATEADD(day, -1, CAST(GETDATE() AS date))
GROUP BY
    d.UEN
""", conexMSSQL)

df_despachos["UEN"] = df_despachos["UEN"].str.strip()


# Counting all the truckers purchases, of yesterday, that dont have an account
# but use "RedMas" Card
df_despachosRedmas = pd.read_sql("""
    SELECT
    a.UEN, COUNT(distinct a.DESPACHOS) AS 'DESPACHOS RED MAS'
FROM (
    SELECT 
        D.UEN, D.ID AS 'DESPACHOS'
    FROM
        [Rumaos].[dbo].[VIEW_DESPAPRO_FILTRADA] AS D
    INNER JOIN
        Rumaos.dbo.Fac001 AS F
    ON
        D.TIPOCOMP = F.TIPOCOMP
        AND D.LETRACOMP = F.LETRACOMP
        AND D.PTOVTA = F.PTOVTA
        AND D.NROCOMP = F.NROCOMP
    WHERE 
        D.TARJETA LIKE 'i%'
        AND D.CODPRODUCTO NOT LIKE 'GNC'
        AND D.UEN IN ('MERC GUAYMALLEN', 'MERCADO 2', 'PUENTE OLIVE')
        AND D.VOLUMEN >= '80'
        AND D.FECHASQL BETWEEN DATEADD(day, -7, CAST(GETDATE() AS date)) AND DATEADD(day, -1, CAST(GETDATE() AS date))

    UNION

    SELECT    
        D.UEN, D.ID AS 'DESPACHOS'
    FROM
        [Rumaos].[dbo].[VIEW_DESPAPRO_FILTRADA] AS D
    INNER JOIN
        Rumaos.dbo.FacRemDet AS F
    ON 
        D.PTOVTA = F.PTOVTA
        AND D.NROCOMP = F.NROREMITO
    WHERE 
        D.TARJETA LIKE 'i%'
        AND D.CODPRODUCTO NOT LIKE 'GNC'
        AND D.UEN IN ('MERC GUAYMALLEN', 'MERCADO 2', 'PUENTE OLIVE')
        AND D.VOLUMEN >= '80'
        AND D.FECHASQL BETWEEN DATEADD(day, -7, CAST(GETDATE() AS date)) AND DATEADD(day, -1, CAST(GETDATE() AS date))
) AS a
GROUP BY
    a.UEN

UNION ALL

SELECT
    d.UEN, COUNT(DISTINCT d.ID) AS 'DESPACHOS RED MAS'
FROM Despapro AS d
WHERE
    D.TARJETA LIKE 'i%'
    AND d.UEN IN ('PERDRIEL', 'PERDRIEL2')
    AND d.CODPRODUCTO IN ('go', 'eu')
    AND d.aforador NOT LIKE '%[a-zA-Z]%'
    AND d.FECHASQL BETWEEN DATEADD(day, -7, CAST(GETDATE() AS date)) AND DATEADD(day, -1, CAST(GETDATE() AS date))
GROUP BY
    d.UEN
""", conexMSSQL)

df_despachosRedmas["UEN"] = df_despachosRedmas["UEN"].str.strip()


# Merge "df_despachosRedmas" and "df_despachos"
df_despachosCompleto = pd.merge(
    df_despachosRedmas,
    df_despachos,
    on="UEN",
    how="outer"
)

# Create column "PENETRACIÓN RED MÁS" and insert it in position 1
df_despachosCompleto.insert(
    1
    ,"PENETRACIÓN RED MÁS"
    ,df_despachosCompleto.apply(
        lambda row: row["DESPACHOS RED MAS"]/row["DESPACHOS"]
            , axis= 1
    )
)

# Sort by "PENETRACIÓN RED MÁS" in descending order
df_despachosCompleto = df_despachosCompleto.sort_values(
    by=["PENETRACIÓN RED MÁS"]
    ,ascending=False
)

# Merge pivot_1era_carga_camion and df_despachosCompleto
df_despachos_Y_Activ = pd.merge(
    df_despachosCompleto,
    pivot_1era_carga_camion,
    on="UEN",
    how="outer"
)

# Fill NaNs of merges
df_despachos_Y_Activ.fillna(value=0, inplace=True)

# Create row of column totals
df_despachos_Y_Activ.loc["colTOTAL"]= pd.Series(
    df_despachos_Y_Activ.sum()
    , index=["DESPACHOS RED MAS","DESPACHOS","ACTIVACIONES"]
)

# Fill NaN in UEN column at total row
df_despachos_Y_Activ.fillna({"UEN":"TOTAL"}, inplace=True)

# Fill NaN in "PENETRACIÓN RED MÁS" column at total row with the total quantity 
# of "DESPACHOS RED MAS" over "DESPACHOS"
tasa = (df_despachos_Y_Activ.loc["colTOTAL","DESPACHOS RED MAS"] /
    df_despachos_Y_Activ.loc["colTOTAL","DESPACHOS"])

df_despachos_Y_Activ.fillna({"PENETRACIÓN RED MÁS":tasa}, inplace=True)

# Creating column "ACTIVACIONES" with value zero in case it doesn't exist
if "ACTIVACIONES" not in df_despachos_Y_Activ:
    df_despachos_Y_Activ["ACTIVACIONES"] = 0

df_despachos_Y_Activ = df_despachos_Y_Activ.reindex(columns=['UEN','PENETRACIÓN RED MÁS','DESPACHOS RED MAS','DESPACHOS'])

##########################################
# STYLING of the dataframe
##########################################

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
        .set_caption(
            titulo
            + "<br>"
            + "Semana Actual "
            + ((pd.to_datetime("today")-pd.to_timedelta(7,"days"))
            .strftime("%d/%m/%y"))
            + " al "
            + ((pd.to_datetime("today")-pd.to_timedelta(1,"days"))
            .strftime("%d/%m/%y"))
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
        .applymap(lambda x: 'color: blue' if x >= 0.75 else 'color: #FF0000', subset=pd.IndexSlice[:, ['PENETRACIÓN RED MÁS']]) \
        .apply(lambda x: ["background: black" if x.name == "colTOTAL" 
            else "" for i in x]
            , axis=1) \
        .apply(lambda x: ["color: white" if x.name == "colTOTAL" 
            else "" for i in x]
            , axis=1)
    return resultado

df_despachos_Y_Activ_Estilo = estiladorVtaTitulo(
    df_despachos_Y_Activ
    ,"PENETRACIÓN RED MÁS"
    ,["DESPACHOS", "DESPACHOS RED MAS"]
    , "RED MÁS CAMIONES"
)

try:
    display(df_despachos_Y_Activ_Estilo) # type: ignore
except:
    pass


##########################################
# PRINTING dataframe as an image
##########################################

# This will print the df with a unique name and will erase the old image 
# everytime the script is run

ubicacion = str(pathlib.Path(__file__).parent)+"\\"
nombreDespachos = "Info_Despachos_Camioneros_Semanal.png"

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


df_to_image(df_despachos_Y_Activ_Estilo, ubicacion, nombreDespachos)


# Timer
tiempoFinal = pd.to_datetime("today")
logger.info(
    "\nInfo 1er Despachos Camioneros"
    + "\nTiempo de Ejecucion Total: "
    + str(tiempoFinal-tiempoInicio)
)


# if __name__ == "__main__":
#     print("Nada por aquí")
#     # Timer
#     tiempoFinal = pd.to_datetime("today")
#     print("\nInfo 1er Despachos Camionero"+"\nTiempo de Ejecucion Total:")
#     print(tiempoFinal-tiempoInicio)  
