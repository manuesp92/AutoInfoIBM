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
    
today = date.today()  # obtiene la fecha actual
start_of_month = date(today.year, today.month, 1)  # obtiene la fecha de inicio del mes actual
start_of_month=start_of_month.strftime('%Y-%m-%d')
today=today.strftime('%Y-%m-%d')

hoy = datetime.now()
ayer = hoy - timedelta(days=1)
primer_dia_mes = ayer.replace(day=1)


now = datetime.now()  # Obtiene la fecha y hora actual
primer_dia_mes_actual = datetime(now.year, now.month, 1)  # Obtiene el primer día del mes actual
ultimo_dia_mes_anterior = primer_dia_mes_actual - timedelta(days=1)  # Resta un día para obtener el último día del mes anterior
primer_dia_mes_anterior = datetime(ultimo_dia_mes_anterior.year, ultimo_dia_mes_anterior.month, 1)  # Obtiene el primer día del mes anterior
fecha_inicio_mes_anterior = primer_dia_mes_anterior.strftime('%Y-%m-%d')
###PRESUPUESTO
sheet_id='1ofJ38O2-dLfw7TZOxeTDYUzlWgl6hN2iMLZCMscTgGo'
hoja='Presupuesto'
gsheet_url_presupuesto = "https://docs.google.com/spreadsheets/d/{}/gviz/tq?tqx=out:csv&sheet={}".format(sheet_id, hoja)
df_presupuesto=pd.read_csv(gsheet_url_presupuesto)
df_presupuesto = df_presupuesto.convert_dtypes()
df_presupuesto['FECHASQL'] = pd.to_datetime(df_presupuesto['FECHASQL'], format='%d/%m/%Y')
df_presupuesto= df_presupuesto.loc[(df_presupuesto["FECHASQL"] <= ayer.strftime('%Y-%m-%d'))& (df_presupuesto["FECHASQL"] >= primer_dia_mes.strftime('%Y-%m-%d')),:]

### TRAMOS Y COSTOS 
sheet_id2='1yJlZkGWDcYa5hdlXZxY5_xbi4s3Y0AqoS-L2QgDdFTQ'

######### LECTURA DE EXCEL DE COSTOS DAPSA EU
sheet_name3= 'CostoDapsaNS'
gsheet_url_costoNS = "https://docs.google.com/spreadsheets/d/{}/gviz/tq?tqx=out:csv&sheet={}".format(sheet_id2, sheet_name3)
costoDapsaNS =pd.read_csv(gsheet_url_costoNS)
costoDapsaNS = costoDapsaNS.convert_dtypes()
costoDapsaNS['FECHASQL'] = pd.to_datetime(costoDapsaNS['FECHASQL'], format='%d/%m/%Y')


######### LECTURA DE EXCEL DE COSTOS DAPSA EU
sheet_name4= 'CostoDapsaNU'
gsheet_url_costoNU = "https://docs.google.com/spreadsheets/d/{}/gviz/tq?tqx=out:csv&sheet={}".format(sheet_id2, sheet_name4)
costoDapsaNU =pd.read_csv(gsheet_url_costoNU)
costoDapsaNU = costoDapsaNU.convert_dtypes()
costoDapsaNU['FECHASQL'] = pd.to_datetime(costoDapsaNU['FECHASQL'], format='%d/%m/%Y')


######### LECTURA DE EXCEL DE COMISIONES Y TRAMOS NS
sheet_name1= 'ComisionyTramoNS'
gsheet_url_comisionNS = "https://docs.google.com/spreadsheets/d/{}/gviz/tq?tqx=out:csv&sheet={}".format(sheet_id2, sheet_name1)
comisionNS =pd.read_csv(gsheet_url_comisionNS)
comisionNS = comisionNS.convert_dtypes()
comisionNS['Fecha'] = pd.to_datetime(comisionNS['Fecha'], format='%d/%m/%Y')
comisionNS= comisionNS.loc[comisionNS["Fecha"] == primer_dia_mes.strftime('%Y-%m-%d'),:]


######### LECTURA DE EXCEL DE COMISIONES Y TRAMOS EU
sheet_name2= 'ComisionyTramoNU'
gsheet_url_comisionNU = "https://docs.google.com/spreadsheets/d/{}/gviz/tq?tqx=out:csv&sheet={}".format(sheet_id2, sheet_name2)
comisionNU =pd.read_csv(gsheet_url_comisionNU)
comisionNU = comisionNU.convert_dtypes()
comisionNU['Fecha'] = pd.to_datetime(comisionNU['Fecha'], format='%d/%m/%Y')
comisionNU= comisionNU.loc[comisionNU["Fecha"] == primer_dia_mes.strftime('%Y-%m-%d'),:]

df_presupuesto = df_presupuesto.reindex(columns=['UEN','FECHASQL','CODPRODUCTO','VENTAS'])


##########################################
# TRABAJO CON TABLA DE EXCEL    ################
##########################################

df_presupuestoNAFTA=df_presupuesto.loc[(df_presupuesto['CODPRODUCTO']=='NS') | (df_presupuesto['CODPRODUCTO']=='NU')]
egnctotales =df_presupuestoNAFTA


egncTotal = df_presupuestoNAFTA
egncTotal = egncTotal.rename({'Fecha':'FECHASQL', 'VENTAS':'Presupuesto Diario'},axis=1)
egncTotal = egncTotal.reindex(columns=['UEN','FECHASQL','CODPRODUCTO','Presupuesto Diario'])


################################################
############# Volumen diario GASOLEOS YPF 
################################################

df_naftasTotal = pd.read_sql(f'''
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
        
    SELECT UEN  
        ,[CODPRODUCTO]
        ,SUM([VTATOTVOL]) AS 'VENTA TOTAL VOLUMEN'
		,FECHASQL
		,sum(VTAPRECARTELVOL) AS 'Ventas Efectivo'
		,sum(VTACTACTEVOL + VTAADELVOL) AS 'Venta Cta Cte'
		,sum(VTAPROMOSVOL) AS 'ventas Promos'
    FROM [Rumaos].[dbo].[EmpVenta]
    WHERE FECHASQL >= '{primer_dia_mes.strftime('%Y-%d-%m')}'
        AND FECHASQL <= '{ayer.strftime('%Y-%d-%m')}'
	    AND VTATOTVOL > '0'
		and (CODPRODUCTO = 'NS' OR CODPRODUCTO = 'NU')
		and UEN IN (
            'LAMADRID'
            ,'AZCUENAGA'
            ,'PERDRIEL'
            ,'PERDRIEL2'
            ,'PUENTE OLIVE'
            ,'SAN JOSE'
        )
		group BY FECHASQL,CODPRODUCTO,UEN
		order by CODPRODUCTO,UEN

  '''      ,db_conex)
df_naftasTotal = df_naftasTotal.convert_dtypes()


############################################
#####Volumen Diario GASOLEOS REDMAS APPYPF######
############################################

df_naftasREDMAS = pd.read_sql(f'''
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
	SELECT emp.UEN,emp.FECHASQL,sum(-emp.VOLUMEN) as 'Pruebas Surtidor',emp.CODPRODUCTO
    FROM [Rumaos].[dbo].[EmpPromo] AS EmP
        INNER JOIN Promocio AS P 
            ON EmP.UEN = P.UEN 
            AND EmP.CODPROMO = P.CODPROMO
    WHERE FECHASQL >= '{primer_dia_mes.strftime('%Y-%d-%m')}'
        AND FECHASQL <= '{ayer.strftime('%Y-%d-%m')}'
		and emp.UEN IN (
            'LAMADRID'
            ,'AZCUENAGA'
            ,'PERDRIEL'
            ,'PERDRIEL2'
            ,'PUENTE OLIVE'
            ,'SAN JOSE'
        )
		and (emp.CODPRODUCTO = 'NS' OR emp.CODPRODUCTO = 'NU')
        AND  (P.[DESCRIPCION] like '%PRUEBA%')
		group by emp.FECHASQL,emp.CODPRODUCTO,emp.UEN
		order by CODPRODUCTO,UEN
'''      ,db_conex)
df_naftasREDMAS = df_naftasREDMAS.convert_dtypes()


df_naftasTotal=df_naftasTotal.merge(df_naftasREDMAS,on=['UEN','FECHASQL','CODPRODUCTO'],how='outer')

########################################################################################################
df_naftasTotal = df_naftasTotal.fillna(0)

df_naftasTotal['Volumen Total Vendido']=((df_naftasTotal['VENTA TOTAL VOLUMEN']+df_naftasTotal['Pruebas Surtidor']))

presupuestoYPF = egncTotal.loc[(egncTotal['UEN'] == 'LAMADRID') | (egncTotal['UEN'] == 'AZCUENAGA') | (egncTotal['UEN'] == 'PERDRIEL')
                               | (egncTotal['UEN'] == 'PERDRIEL2') | (egncTotal['UEN'] == 'SAN JOSE') | (egncTotal['UEN'] == 'PUENTE OLIVE'),:]

df_naftasTotal['UEN']=df_naftasTotal['UEN'].str.strip()
df_naftasTotal['CODPRODUCTO']=df_naftasTotal['CODPRODUCTO'].str.strip()

df_naftasTotalSinPresupuesto= df_naftasTotal

df_naftasTotal = df_naftasTotal.merge(presupuestoYPF,on=['UEN','FECHASQL','CODPRODUCTO'],how='outer')


df_naftasTotal = df_naftasTotal.reindex(columns=['UEN','CODPRODUCTO','Volumen Total Vendido','Presupuesto Diario'])
df_naftasTotal = df_naftasTotal.fillna(0)
#df_naftasTotal= df_naftasTotal.loc[df_naftasTotal["FECHASQL"] <= ayer.strftime('%Y-%m-%d'),:]

df_naftasTotal=df_naftasTotal.sort_values(['UEN','CODPRODUCTO'])

df_naftasTotal= df_naftasTotal.groupby(
        ["UEN",'CODPRODUCTO']
        , as_index=False
    ).sum(numeric_only=True)

### Concateno Tabla De YPF NS con YPF EU
mbcYPF=df_naftasTotal


##################################################
####### VENTAS DAPSA #############################
##################################################

####  Volumen diario GASOLEOS YPF 
df_naftasdapsa = pd.read_sql(f''' 
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
        
    SELECT UEN  
        ,[CODPRODUCTO]
        ,SUM([VTATOTVOL]) AS 'VENTA TOTAL VOLUMEN'
		,FECHASQL
		,sum(VTAPRECARTELVOL) AS 'Ventas Efectivo'
		,sum(VTACTACTEVOL + VTAADELVOL) AS 'Venta Cta Cte'
		,sum(VTAPROMOSVOL) AS 'ventas Promos'
    FROM [Rumaos].[dbo].[EmpVenta]
    WHERE FECHASQL >= '{primer_dia_mes.strftime('%Y-%d-%m')}'
        AND FECHASQL <= '{ayer.strftime('%Y-%d-%m')}'
	    AND VTATOTVOL > '0'
		and (CODPRODUCTO = 'NS' OR CODPRODUCTO = 'NU')
		and UEN IN (
            'SARMIENTO'
            ,'URQUIZA'
            ,'MITRE'
            ,'MERC GUAYMALLEN'
			,'ADOLFO CALLE'
			,'VILLANUEVA'
			,'MERCADO 2'
			,'LAS HERAS'
        )
		group BY FECHASQL,CODPRODUCTO,UEN
		order by CODPRODUCTO,UEN

   '''  ,db_conex)
df_naftasdapsa = df_naftasdapsa.convert_dtypes()
df_naftasdapsa =df_naftasdapsa.fillna(0)
### Descuentos
df_naftadapsaDesc = pd.read_sql(f'''

            DECLARE @ayer DATETIME
	SET @ayer = DATEADD(day, -1, CAST(GETDATE() AS date))
	DECLARE @inicioMesActual DATETIME
	SET @inicioMesActual = DATEADD(month, DATEDIFF(month, 0, @ayer), 0)

	DECLARE @inicioMesAnterior DATETIME
	SET @inicioMesAnterior = DATEADD(M,-1,@inicioMesActual)

	--Divide por la cant de días del mes anterior y multiplica por la cant de días del
	--mes actual
	
	DECLARE @hoy DATETIME
	SET @hoy = DATEADD(DAY, DATEDIFF(DAY, 1, CURRENT_TIMESTAMP), 0)
	SELECT emp.UEN,EmP.FECHASQL
        ,EmP.[CODPRODUCTO]
        ,SUM(-EmP.[VOLUMEN]) as 'Descuentos'
    FROM [Rumaos].[dbo].[EmpPromo] AS EmP
        INNER JOIN Promocio AS P 
            ON EmP.UEN = P.UEN 
            AND EmP.CODPROMO = P.CODPROMO
    WHERE FECHASQL >= '{primer_dia_mes.strftime('%Y-%d-%m')}'
        AND FECHASQL <= '{ayer.strftime('%Y-%d-%m')}'
		and emp.UEN IN (
            'SARMIENTO'
            ,'URQUIZA'
            ,'MITRE'
            ,'MERC GUAYMALLEN'
			,'ADOLFO CALLE'
			,'VILLANUEVA'
			,'MERCADO 2'
			,'LAS HERAS'
        )
		and (emp.CODPRODUCTO = 'NS' OR emp.CODPRODUCTO = 'NU')
        AND (EmP.[CODPROMO] = '30'
            OR P.[DESCRIPCION] like '%PRUEBA%')
		GROUP BY emp.UEN,EMP.FECHASQL,EMP.CODPRODUCTO

  '''      ,db_conex)
df_naftadapsaDesc = df_naftadapsaDesc.convert_dtypes()
### CONCATENO TABLA DE VOLUMEN TOTAL VENDIDO CON LA TABLA DE LOS DESCUENTOS
df_naftasdapsa = df_naftasdapsa.merge(df_naftadapsaDesc, on=['UEN','FECHASQL','CODPRODUCTO'],how='outer')
df_naftasdapsa =df_naftasdapsa.fillna(0)


df_naftasdapsa['Volumen Total Vendido']=(df_naftasdapsa['Ventas Efectivo']+df_naftasdapsa['Venta Cta Cte']
                                           +(df_naftasdapsa['ventas Promos']+df_naftasdapsa['Descuentos']))


presupuestoDAPSA = egncTotal.loc[~((egncTotal['UEN'] == 'LAMADRID') | (egncTotal['UEN'] == 'AZCUENAGA') | (egncTotal['UEN'] == 'PERDRIEL')
                               | (egncTotal['UEN'] == 'PERDRIEL2') | (egncTotal['UEN'] == 'SAN JOSE') | (egncTotal['UEN'] == 'PUENTE OLIVE')) ,:]

df_naftasdapsa['UEN']=df_naftasdapsa['UEN'].str.strip()
df_naftasdapsa['CODPRODUCTO']=df_naftasdapsa['CODPRODUCTO'].str.strip()

df_naftasdapsa = df_naftasdapsa.reindex(columns=['UEN','CODPRODUCTO','Volumen Total Vendido','Presupuesto Diario',])
df_naftasdapsa = df_naftasdapsa.fillna(0)

df_naftasdapsa=df_naftasdapsa.sort_values(['UEN','CODPRODUCTO'])
df_naftasdapsa= df_naftasdapsa.groupby(
        ["UEN",'CODPRODUCTO']
        , as_index=False
    ).sum(numeric_only=True)
df_naftasdapsa=df_naftasdapsa[df_naftasdapsa['Presupuesto Diario']!=0]
mbcTOTAL=pd.concat([df_naftasTotal, df_naftasdapsa])
mbcTOTAL= df_naftasTotal.merge(df_naftasdapsa, on=['UEN','CODPRODUCTO','Volumen Total Vendido','Presupuesto Diario'], how='outer')

## Creo Dataframe de NS
ns = mbcTOTAL['CODPRODUCTO'] == "NS"
mbcTOTALNS = mbcTOTAL[ns]
mbcTOTALNS = mbcTOTALNS.rename({'Presupuesto Diario':'Presupuesto Acumulado L'},axis=1)
mbcTOTALNS = mbcTOTALNS.reindex(columns=['UEN','Presupuesto Acumulado L','Volumen Total Vendido'])
## Creo Dataframe de EU
nu = mbcTOTAL['CODPRODUCTO'] == "NU"
mbcTotalNU = mbcTOTAL[nu]
mbcTotalNU = mbcTotalNU.rename({'Presupuesto Diario':'Presupuesto Acumulado L'},axis=1)
mbcTotalNU=mbcTotalNU.reindex(columns=['UEN','Presupuesto Acumulado L','Volumen Total Vendido'])



###### Columnas de Desvio y Totales NS
mbcTOTALNS['Desvio Presupuestado L']=(mbcTOTALNS['Volumen Total Vendido']-mbcTOTALNS['Presupuesto Acumulado L'])
mbcTOTALNS['Desvio Presupuestado L %']=(mbcTOTALNS['Volumen Total Vendido']/mbcTOTALNS['Presupuesto Acumulado L'])-1


mbcTOTALNS.loc["colTOTAL"]= pd.Series(
    mbcTOTALNS.sum(numeric_only=True)
    , index=['Presupuesto Acumulado L','Volumen Total Vendido']
)
mbcTOTALNS.fillna({"UEN":"TOTAL"}, inplace=True)

tasa = (mbcTOTALNS.loc["colTOTAL",'Volumen Total Vendido'] -
    mbcTOTALNS.loc["colTOTAL",'Presupuesto Acumulado L'])
mbcTOTALNS.fillna({"Desvio Presupuestado L":tasa}, inplace=True)

tasa2 = (mbcTOTALNS.loc["colTOTAL",'Volumen Total Vendido'] /
    mbcTOTALNS.loc["colTOTAL",'Presupuesto Acumulado L'])-1
mbcTOTALNS.fillna({'Desvio Presupuestado L %':tasa2}, inplace=True)

mbcTOTALNS=mbcTOTALNS.reindex(columns=['UEN','Presupuesto Acumulado L','Volumen Total Vendido','Desvio Presupuestado L','Desvio Presupuestado L %'])

###### Columnas de Desvio y Totales EU
mbcTotalNU['Desvio Presupuestado L']=(mbcTotalNU['Volumen Total Vendido']-mbcTotalNU['Presupuesto Acumulado L'])
mbcTotalNU['Desvio Presupuestado L %']=(mbcTotalNU['Volumen Total Vendido']/mbcTotalNU['Presupuesto Acumulado L'])-1

mbcTotalNU=mbcTotalNU.fillna(0)

mbcTotalNU.loc["colTOTAL"]= pd.Series(
    mbcTotalNU.sum(numeric_only=True)
    , index=['Presupuesto Acumulado L','Volumen Total Vendido']
)
mbcTotalNU.fillna({"UEN":"TOTAL"}, inplace=True)

tasa = (mbcTotalNU.loc["colTOTAL",'Volumen Total Vendido'] -
    mbcTotalNU.loc["colTOTAL",'Presupuesto Acumulado L'])
mbcTotalNU.fillna({"Desvio Presupuestado L":tasa}, inplace=True)

tasa2 = (mbcTotalNU.loc["colTOTAL",'Volumen Total Vendido'] /
    mbcTotalNU.loc["colTOTAL",'Presupuesto Acumulado L'])-1
mbcTotalNU.fillna({'Desvio Presupuestado L %':tasa2}, inplace=True)


mbcTotalNU=mbcTotalNU.reindex(columns=['UEN','Presupuesto Acumulado L','Volumen Total Vendido','Desvio Presupuestado L','Desvio Presupuestado L %'])
######### LE DOY FORMATO AL DATAFRAME

primer_dia_mes.strftime('%Y-%d-%m')
ayer.strftime('%Y-%d-%m')

def _estiladorVtaTituloD(df, list_Col_litros, list_Col_Perc, titulo):
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
        .format("{:,.2%}", subset=list_Col_Perc) \
        .format("{:,.2f} L", subset=list_Col_litros) \
        .hide(axis=0) \
        .set_caption(
            titulo
            + "<br>"
            + (primer_dia_mes.strftime('%Y-%d-%m'))
            + "-"
            + (ayer.strftime('%Y-%d-%m'))
            + "<br>") \
        .applymap(lambda x: 'color: blue' if x >= 0 else 'color: #FF0000', subset=pd.IndexSlice[:, ['Desvio Presupuestado L %']]) \
        .applymap(lambda x: 'color: blue' if x >= 0 else 'color: #FF0000', subset=pd.IndexSlice[:, ['Desvio Presupuestado L']]) \
        .set_properties(subset= list_Col_Perc + list_Col_litros
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

#  columnas sin decimales

numColslitros=['Presupuesto Acumulado L','Volumen Total Vendido','Desvio Presupuestado L']
# Columnas Porcentaje
percColsPen = ['Desvio Presupuestado L %']
### APLICO EL FORMATO A LA TABLA
mbcTOTALNS = _estiladorVtaTituloD(mbcTOTALNS,numColslitros,percColsPen, "Ejecucion Presupuestaria Nafta Super")
mbcTotalNU = _estiladorVtaTituloD(mbcTotalNU,numColslitros,percColsPen, "Ejecucion Presupuestaria Infinia Nafta")


ubicacion= str(pathlib.Path(__file__).parent)+"\\"

nombreNS = "Info_Presupuesto_NS_Acumulado.png"
nombreNU = "Info_Presupuesto_NU_Acumulado.png"
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

df_to_image(mbcTOTALNS, ubicacion, nombreNS)
df_to_image(mbcTotalNU, ubicacion, nombreNU)



#############
# Timer
tiempoFinal = pd.to_datetime("today")
logger.info(
    "\nInfo Volumen de Ventas"
    + "\nTiempo de Ejecucion Total: "
    + str(tiempoFinal-tiempoInicio)
)