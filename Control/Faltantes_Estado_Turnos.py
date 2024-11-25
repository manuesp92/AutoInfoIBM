import os
import math
import numpy as np
from PIL import Image
import pandas as pd
from pandas.api.types import CategoricalDtype
import pyodbc
import pathlib
from pathlib import Path
from datetime import timedelta,datetime,date
import dataframe_image as dfi
import sys
import logging
import matplotlib.pyplot as plt
from pandas.plotting import table

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        , level=logging.INFO
)
logger = logging.getLogger(__name__)

hoy = datetime.now()
ayer = hoy - timedelta(days=1)
primer_dia_mes = hoy.replace(day=1)


# Diccionario para mapear nombres de meses en inglés a español
meses = {
    "January": "Enero",
    "February": "Febrero",
    "March": "Marzo",
    "April": "Abril",
    "May": "Mayo",
    "June": "Junio",
    "July": "Julio",
    "August": "Agosto",
    "September": "Septiembre",
    "October": "Octubre",
    "November": "Noviembre",
    "December": "Diciembre"
}

# Obtener el nombre del mes actual
mes_actual_ingles = datetime.now().strftime('%B')

# Traducir el nombre del mes actual al español
mes_actual_espanol = meses.get(mes_actual_ingles)


server = "192.168.200.33,50020\cloud"
database = "Rumaos"
username = "jbriffe" 
password = "t3GPnmn4"

login = [server,database,username,password]

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
    
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        , level=logging.INFO
)
logger = logging.getLogger(__name__)

tiempoInicio = pd.to_datetime("today")

pd.options.display.float_format = "{:20,.0f}".format 

print("ok")

################################ LECTURA DE CONSULTAS SQL################################

###################### FALTANTES PLAYA ########################################

turnos_playa = pd.read_sql('''	DECLARE @hoy DATE 
	set @hoy = DATEADD(DAY, 0, GETDATE()) 
	SELECT
		A.UEN
		,A.CODPERSONAL
		,A.NROLEGAJO
		,A.NOMBRE
		,SUM(A.faltante) AS 'Faltantes acumulados'
	FROM
			(select ET.UEN
				   ,ET.FECHASQL
				   ,ET.TURNO
				   ,ET.CODPERSONAL
				   ,PU.NROLEGAJO
				   ,PU.NOMBRE
				   ,IIF(((ET.VTAPRECARIMP + ET.VTAPROMOIMP + ET.VTAOTROSIMP + ET.ImpoRecaEW) - ET.RECFISICA)>0,
							((ET.VTAPRECARIMP + ET.VTAPROMOIMP + ET.VTAOTROSIMP + ET.ImpoRecaEW) - ET.RECFISICA),0) as faltante
				   ,IIF(((ET.VTAPRECARIMP + ET.VTAPROMOIMP + ET.VTAOTROSIMP + ET.ImpoRecaEW) - ET.RECFISICA)<0,
							((ET.VTAPRECARIMP + ET.VTAPROMOIMP + ET.VTAOTROSIMP + ET.ImpoRecaEW) - ET.RECFISICA),0) as sobrante
				   ,VT.ESTADO as nro_estado
				   ,iif(VT.ESTADO = 2, 'Cerrado', 'Abierto') as Estado
				   ,VT.ESTAUDITORIA as nro_estado_aud
				   ,iif(VT.ESTAUDITORIA = 2, 'Auditado', 'No Auditado') as Estado_Audit
				   from EmpTurno as ET
					left join Vtaturno as VT ON ET.UEN = VT.UEN AND ET.FECHASQL = VT.FECHASQL AND ET.TURNO = VT.TURNO
							left join PersonalUEN as PU ON ET.UEN = PU.UEN AND ET.CODPERSONAL = PU.CODPERSONAL
	
			WHERE (month(ET.FECHASQL) = month(@hoy) and year(ET.FECHASQL) = year(@hoy) )
			) as A
		WHERE A.Estado_Audit = 'Auditado'
		GROUP BY 
		 A.UEN
		,A.CODPERSONAL
		,A.NROLEGAJO
		,A.NOMBRE
		order by   'Faltantes acumulados' desc''',db_conex)

turnos_playa = turnos_playa.convert_dtypes()
turnos_playa['Faltantes acumulados'] = pd.to_numeric(turnos_playa['Faltantes acumulados'], errors='coerce', downcast='float')

###################### FALTANTES FULL ########################################

turnos_full = pd.read_sql('''	DECLARE @hoy DATE 
	set @hoy = DATEADD(DAY, 0, GETDATE()) 
SELECT
		A.UEN
		,A.CODPERSONAL
		,A.NROLEGAJO
		,A.NOMBRE
		,SUM(A.faltante) AS 'Faltantes acumulados'
	FROM(

select				SC.UEN
				   ,SC.FECHASQL
				   ,SC.TURNO
				   ,SC.CODPERSONAL
				   ,PU.NROLEGAJO
				   ,PU.NOMBRE
				   ,IIF((SC.IMPVENTATOTAL-SC.IMPRECREAL)>0,
							(SC.IMPVENTATOTAL-SC.IMPRECREAL),0) as faltante
				   ,IIF((SC.IMPVENTATOTAL-SC.IMPRECREAL)<0,
							(SC.IMPVENTATOTAL-SC.IMPRECREAL),0) as sobrante
				   ,ST.ESTADO as nro_estado
				   ,iif(ST.ESTADO = 2, 'Cerrado', 'Abierto') as Estado
				   ,ST.AUDITADO as nro_estado_aud
				   ,iif(ST.AUDITADO = 1, 'Auditado', 'No Auditado') as Estado_Audit
				   from EMPVTASC as SC
					left join SCTurnos as ST ON SC.UEN = ST.UEN AND SC.FECHASQL = ST.FECHASQL AND SC.TURNO = ST.TURNO
							left join PersonalUEN as PU ON SC.UEN = PU.UEN AND SC.CODPERSONAL = PU.CODPERSONAL
	
			WHERE (month(SC.FECHASQL) = month(@hoy) and year(SC.FECHASQL) = year(@hoy))
			) AS A

		WHERE A.Estado_Audit = 'Auditado'
		GROUP BY A.UEN
		,A.CODPERSONAL
		,A.NROLEGAJO
		,A.NOMBRE
		ORDER BY 'Faltantes acumulados' DESC''',db_conex)
turnos_full = turnos_full.convert_dtypes()
turnos_full['Faltantes acumulados'] = pd.to_numeric(turnos_full['Faltantes acumulados'], errors='coerce', downcast='float')

                ############# TOP 20 PLAYA ##################
                
# Seleccionar las primeras 20 filas del DataFrame
top_20_playa = turnos_playa.head(20)
# 2. Personalizar los encabezados de columna
top_20_playa.columns = ['UEN', 'Codigo personal', 'Legajo', 'Nombre', 'Faltante acumulado']
 
                ############### TOP 20 FULL ##################
                
# Seleccionar las primeras 20 filas del DataFrame
top_20_full = turnos_full.head(20)
# 2. Personalizar los encabezados de columna
top_20_full.columns = ['UEN', 'Codigo personal', 'Legajo', 'Nombre', 'Faltante acumulado']

###################### TURNOS PLAYA HOY ########################################
#GENERO 3 TABLAS CON LOS TURNOS CONDICIONADOS Y LAS UNO
t1p_hoy = pd.read_sql('''DECLARE @ayer DATE
                            SET @ayer = DATEADD(DAY, -1, GETDATE())
                            DECLARE @HOY DATE
                            SET @HOY = DATEADD(DAY, 0, GETDATE())
                    select uen
                           ,iif(ESTADO = 1,'Abierto'
                            ,IIF(ESTADO = 2 and ESTAUDITORIA = 1 and NRORECA = 0,'No Auditado (No Recaudado)'
                             , iif(ESTADO = 2 and ESTAUDITORIA = 2 AND NRORECA <> 0, 'Auditado'
                              , iif (ESTADO = 2 and ESTAUDITORIA = 1 and NRORECA <> 0, 'No Auditado (Recaudado)','')))) as  'estado turno 1'  from VtaTurno
                    where fechasql = @HOY AND TURNO = 1
                    order by UEN ''',db_conex)
t2p_hoy  = pd.read_sql('''DECLARE @ayer DATE
                            SET @ayer = DATEADD(DAY, -1, GETDATE())
                            DECLARE @HOY DATE
                            SET @HOY = DATEADD(DAY, 0, GETDATE())
                    select uen
                           ,iif(ESTADO = 1,'Abierto'
                            ,IIF(ESTADO = 2 and ESTAUDITORIA = 1 and NRORECA = 0,'No Auditado (No Recaudado)'
                             , iif(ESTADO = 2 and ESTAUDITORIA = 2 AND NRORECA <> 0, 'Auditado'
                              , iif (ESTADO = 2 and ESTAUDITORIA = 1 and NRORECA <> 0, 'No Auditado (Recaudado)','')))) as  'estado turno 2'  from VtaTurno
                    where fechasql = @HOY AND TURNO = 2
                    order by UEN  ''',db_conex)
t3p_hoy  = pd.read_sql('''DECLARE @ayer DATE
                            SET @ayer = DATEADD(DAY, -1, GETDATE())
                            DECLARE @HOY DATE
                            SET @HOY = DATEADD(DAY, 0, GETDATE())
                    select uen
                           ,iif(ESTADO = 1,'Abierto'
                            ,IIF(ESTADO = 2 and ESTAUDITORIA = 1 and NRORECA = 0,'No Auditado (No Recaudado)'
                             , iif(ESTADO = 2 and ESTAUDITORIA = 2 AND NRORECA <> 0, 'Auditado'
                              , iif (ESTADO = 2 and ESTAUDITORIA = 1 and NRORECA <> 0, 'No Auditado (Recaudado)','')))) as  'estado turno 3'  from VtaTurno
                    where fechasql = @HOY AND TURNO = 3
                    order by UEN   ''',db_conex)
# Realizar un merge entre t1 y t2
t1_y_t2_pl_hoy = pd.merge(t1p_hoy, t2p_hoy, on='uen', how='left')
# Realizar un merge entre el resultado anterior y t3
turnos_pl_hoy = pd.merge(t1_y_t2_pl_hoy, t3p_hoy, on='uen', how='left')
turnos_pl_hoy = turnos_pl_hoy.fillna("Sin abrir")


###################### TURNOS PLAYA AYER ########################################
#GENERO 3 TABLAS CON LOS TURNOS CONDICIONADOS Y LAS UNO
t1p_ayer = pd.read_sql('''DECLARE @ayer DATE
                            SET @ayer = DATEADD(DAY, -1, GETDATE())
                            DECLARE @HOY DATE
                            SET @HOY = DATEADD(DAY, 0, GETDATE())
                    select uen
                           ,iif(ESTADO = 1,'Abierto'
                            ,IIF(ESTADO = 2 and ESTAUDITORIA = 1 and NRORECA = 0,'No Auditado (No Recaudado)'
                             , iif(ESTADO = 2 and ESTAUDITORIA = 2 AND NRORECA <> 0, 'Auditado'
                              , iif (ESTADO = 2 and ESTAUDITORIA = 1 and NRORECA <> 0, 'No Auditado (Recaudado)','')))) as  'estado turno 1'  from VtaTurno
                    where fechasql = @ayer AND TURNO = 1
                    order by UEN ''',db_conex)
t2p_ayer  = pd.read_sql('''DECLARE @ayer DATE
                            SET @ayer = DATEADD(DAY, -1, GETDATE())
                            DECLARE @HOY DATE
                            SET @HOY = DATEADD(DAY, 0, GETDATE())
                    select uen
                           ,iif(ESTADO = 1,'Abierto'
                            ,IIF(ESTADO = 2 and ESTAUDITORIA = 1 and NRORECA = 0,'No Auditado (No Recaudado)'
                             , iif(ESTADO = 2 and ESTAUDITORIA = 2 AND NRORECA <> 0, 'Auditado'
                              , iif (ESTADO = 2 and ESTAUDITORIA = 1 and NRORECA <> 0, 'No Auditado (Recaudado)','')))) as  'estado turno 2'  from VtaTurno
                    where fechasql = @ayer AND TURNO = 2
                    order by UEN  ''',db_conex)
t3p_ayer  = pd.read_sql('''DECLARE @ayer DATE
                            SET @ayer = DATEADD(DAY, -1, GETDATE())
                            DECLARE @HOY DATE
                            SET @HOY = DATEADD(DAY, 0, GETDATE())
                    select uen
                           ,iif(ESTADO = 1,'Abierto'
                            ,IIF(ESTADO = 2 and ESTAUDITORIA = 1 and NRORECA = 0,'No Auditado (No Recaudado)'
                             , iif(ESTADO = 2 and ESTAUDITORIA = 2 AND NRORECA <> 0, 'Auditado'
                              , iif (ESTADO = 2 and ESTAUDITORIA = 1 and NRORECA <> 0, 'No Auditado (Recaudado)','')))) as  'estado turno 3'  from VtaTurno
                    where fechasql = @ayer AND TURNO = 3
                    order by UEN   ''',db_conex)

# Realizar un merge entre t1 y t2
t1_y_t2_pl_ayer = pd.merge(t1p_ayer, t2p_ayer, on='uen', how='left')
# Realizar un merge entre el resultado anterior y t3
turnos_pl_ayer = pd.merge(t1_y_t2_pl_ayer, t3p_ayer, on='uen', how='left')
turnos_pl_ayer = turnos_pl_ayer.fillna("Sin abrir")

###################### TURNOS FULL HOY ########################################
#GENERO 3 TABLAS CON LOS TURNOS CONDICIONADOS Y LAS UNO
t1sc_hoy = pd.read_sql('''DECLARE @ayer DATE
                SET @ayer = DATEADD(DAY, -1, GETDATE())
                DECLARE @HOY DATE
                SET @HOY = DATEADD(DAY, 0, GETDATE())
                select uen
                       ,iif(ESTADO = 1,'Abierto'
                            ,IIF(ESTADO = 2 and AUDITADO = 0 and NRORECA <= 1,'No Auditado (No Recaudado)'
                                , iif(ESTADO = 2 and AUDITADO = 1 AND NRORECA > 1, 'Auditado'
                                    , iif (ESTADO = 2 and AUDITADO = 0 and NRORECA > 1, 'No Auditado (Recaudado)','')))) as  'estado turno 1' 
					   from scturnos
                        where 
                        FECHASQL = @hoy
                        and turno = 1
                        and uen in ('XPRESS','AZCUENAGA','SAN JOSE','LAMADRID','PUENTE OLIVE','PERDRIEL','PERDRIEL2')''',db_conex)
t2sc_hoy  = pd.read_sql('''DECLARE @ayer DATE
                SET @ayer = DATEADD(DAY, -1, GETDATE())
                DECLARE @HOY DATE
                SET @HOY = DATEADD(DAY, 0, GETDATE())
                select uen
                       ,iif(ESTADO = 1,'Abierto'
                            ,IIF(ESTADO = 2 and AUDITADO = 0 and NRORECA <= 1,'No Auditado (No Recaudado)'
                                , iif(ESTADO = 2 and AUDITADO = 1 AND NRORECA > 1, 'Auditado'
                                    , iif (ESTADO = 2 and AUDITADO = 0 and NRORECA > 1, 'No Auditado (Recaudado)','')))) as  'estado turno 2' 
					   from scturnos
                        where 
                        FECHASQL = @hoy
                        and turno = 2
                        and uen in ('XPRESS','AZCUENAGA','SAN JOSE','LAMADRID','PUENTE OLIVE','PERDRIEL','PERDRIEL2')''',db_conex)
t3sc_hoy  = pd.read_sql('''DECLARE @ayer DATE
                SET @ayer = DATEADD(DAY, -1, GETDATE())
                DECLARE @HOY DATE
                SET @HOY = DATEADD(DAY, 0, GETDATE())
                select uen
                       ,iif(ESTADO = 1,'Abierto'
                            ,IIF(ESTADO = 2 and AUDITADO = 0 and NRORECA <= 1,'No Auditado (No Recaudado)'
                                , iif(ESTADO = 2 and AUDITADO = 1 AND NRORECA > 1, 'Auditado'
                                    , iif (ESTADO = 2 and AUDITADO = 0 and NRORECA > 1, 'No Auditado (Recaudado)','')))) as  'estado turno 3' 
					   from scturnos
                        where 
                        FECHASQL = @hoy
                        and turno = 3
                        and uen in ('XPRESS','AZCUENAGA','SAN JOSE','LAMADRID','PUENTE OLIVE','PERDRIEL','PERDRIEL2')''',db_conex)
# Suponiendo que los DataFrames t1, t2 y t3 ya han sido creados con la lógica proporcionada

# Realizar un merge entre t1 y t2
t1_y_t2_sc_hoy = pd.merge(t1sc_hoy, t2sc_hoy, on='uen', how='left')

# Realizar un merge entre el resultado anterior y t3
turnos_Sc_hoy = pd.merge(t1_y_t2_sc_hoy, t3sc_hoy, on='uen', how='left')
turnos_Sc_hoy = turnos_Sc_hoy.fillna("Sin abrir")


######################TURNOS FULL AYER########################################
#GENERO 3 TABLAS CON LOS TURNOS CONDICIONADOS Y LAS UNO
t1sc = pd.read_sql('''DECLARE @ayer DATE
                SET @ayer = DATEADD(DAY, -1, GETDATE())
                DECLARE @HOY DATE
                SET @HOY = DATEADD(DAY, 0, GETDATE())
                select uen
                       ,iif(ESTADO = 1,'Abierto'
                            ,IIF(ESTADO = 2 and AUDITADO = 0 and NRORECA <= 1,'No Auditado (No Recaudado)'
                                , iif(ESTADO = 2 and AUDITADO = 1 AND NRORECA > 1, 'Auditado'
                                    , iif (ESTADO = 2 and AUDITADO = 0 and NRORECA > 1, 'No Auditado (Recaudado)','')))) as  'estado turno 1' 
                       from scturnos
                        where 
                        FECHASQL = @ayer 
                        and turno = 1
                        and uen in ('XPRESS','AZCUENAGA','SAN JOSE','LAMADRID','PUENTE OLIVE','PERDRIEL','PERDRIEL2')''',db_conex)
t2sc  = pd.read_sql('''DECLARE @ayer DATE
                SET @ayer = DATEADD(DAY, -1, GETDATE())
                DECLARE @HOY DATE
                SET @HOY = DATEADD(DAY, 0, GETDATE())
                select uen
                       ,iif(ESTADO = 1,'Abierto'
                            ,IIF(ESTADO = 2 and AUDITADO = 0 and NRORECA <= 1,'No Auditado (No Recaudado)'
                                , iif(ESTADO = 2 and AUDITADO = 1 AND NRORECA > 1, 'Auditado'
                                    , iif (ESTADO = 2 and AUDITADO = 0 and NRORECA > 1, 'No Auditado (Recaudado)','')))) as  'estado turno 2' 
                       from scturnos
                        where 
                        FECHASQL = @ayer 
                        and turno = 2
                        and uen in ('XPRESS','AZCUENAGA','SAN JOSE','LAMADRID','PUENTE OLIVE','PERDRIEL','PERDRIEL2')''',db_conex)
t3sc  = pd.read_sql('''DECLARE @ayer DATE
                SET @ayer = DATEADD(DAY, -1, GETDATE())
                DECLARE @HOY DATE
                SET @HOY = DATEADD(DAY, 0, GETDATE())
               select uen
                       ,iif(ESTADO = 1,'Abierto'
                            ,IIF(ESTADO = 2 and AUDITADO = 0 and NRORECA <= 1,'No Auditado (No Recaudado)'
                                , iif(ESTADO = 2 and AUDITADO = 1 AND NRORECA > 1, 'Auditado'
                                    , iif (ESTADO = 2 and AUDITADO = 0 and NRORECA > 1, 'No Auditado (Recaudado)','')))) as  'estado turno 3' 
                       from scturnos
                        where 
                        FECHASQL = @ayer 
                        and turno = 3
                        and uen in ('XPRESS','AZCUENAGA','SAN JOSE','LAMADRID','PUENTE OLIVE','PERDRIEL','PERDRIEL2')''',db_conex)
# Suponiendo que los DataFrames t1, t2 y t3 ya han sido creados con la lógica proporcionada

# Realizar un merge entre t1 y t2
t1_y_t2_sc = pd.merge(t1sc, t2sc, on='uen', how='left')

# Realizar un merge entre el resultado anterior y t3
turnos_Sc = pd.merge(t1_y_t2_sc, t3sc, on='uen', how='left')
turnos_Sc = turnos_Sc.fillna("Sin abrir") 

####################### ESTILIZADOR 1 #############################################
def asignar_color(valor):
    if valor == 'Abierto':
        return 'color: red'
    elif valor == 'No Auditado (No Recaudado)':
        return 'color: #E3BE02'
    elif valor == 'Auditado':
        return 'color: green'
    elif valor == 'No Auditado (Recaudado)':
        return 'color: blue'
    else:
        return ''

def _estiladorVtaTituloD(df, col, titulo):
    resultado = df.style \
        .hide(axis=0)\
        .set_caption(
            titulo
            + "<br>"
            + ((tiempoInicio-pd.to_timedelta(1, "days")).strftime("%d/%m/%y"))
            + "<br>") \
        .set_properties(subset=col, **{"text-align": "center", "width": "100px"}) \
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
                    ("background-color", "darkblue"),
                    ("color", "white")
                ]
            },
        ]) \
        .applymap(asignar_color, subset=col)  # Cambiar nombre de columna según corresponda

    evitarTotales = df.index.get_level_values(0)
    return resultado

col = ['estado turno 1', 'estado turno 2', 'estado turno 3']  # Columnas sin decimales

####################### ESTILIZADOR 2 #############################################
def asignar_color2(valor):
    if valor == 'Abierto':
        return 'color: red'
    elif valor == 'No Auditado (No Recaudado)':
        return 'color: #E3BE02'
    elif valor == 'Auditado':
        return 'color: green'
    elif valor == 'No Auditado (Recaudado)':
        return 'color: blue'
    else:
        return ''

def _estiladorVtaTituloD2(df, col, titulo):
    resultado = df.style \
        .hide(axis=0) \
        .set_caption(
            titulo
            + "<br>"
            + tiempoInicio.strftime("%d/%m/%y")
            + "<br>") \
        .set_properties(subset=col, **{"text-align": "center", "width": "100px"}) \
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
                    ("background-color", "darkblue"),
                    ("color", "white")
                ]
            },
        ]) \
        .applymap(asignar_color2, subset=col)  # Cambiar nombre de columna según corresponda
    evitarTotales = df.index.get_level_values(0)
    return resultado

col = ['estado turno 1', 'estado turno 2', 'estado turno 3']  # Columnas sin decimales

####################### ESTILIZADOR 3 #############################################

def asignar_color3(valor):
    if valor > 0:
        return 'color: red'
    elif valor == 0:
        return 'color: black'
    elif valor < 0:
        return 'color: blue'

def _estiladorVtaTituloD3(df, numCols, list_Col_Perc, titulo):

    resultado = df.style \
        .format("{0:,.2f}", subset=numCols) \
        .hide(axis=0) \
        .set_caption(
            titulo
            + "<br>"
            + str(mes_actual_espanol)
            + "<br>") \
        .set_properties(subset=list_Col_Perc + numCols, **{"text-align": "center", "width": "100px"}) \
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
                    ("background-color", "darkblue"),
                    ("color", "white")
                ]
            }
        ]) \
        .applymap(asignar_color3, subset=pd.IndexSlice['Faltante acumulado'])  # Cambiar nombre de columna según corresponda
    evitarTotales = df.index.get_level_values(0)
    
    return resultado


numCols = ['Faltante acumulado'] # Columnas sin decimales

percColsPen = [] # Columnas Porcentaje



####################GENERACION DE TABLAS ESTILIZADAS#####################

turnos_pl_ayer = _estiladorVtaTituloD(turnos_pl_ayer, col, "Estado turnos Playa")
turnos_pl_hoy = _estiladorVtaTituloD2(turnos_pl_hoy, col, "Estado turnos Playa")
turnos_Sc_hoy = _estiladorVtaTituloD2(turnos_Sc_hoy, col, "Estado turnos Full's")
turnos_Sc = _estiladorVtaTituloD(turnos_Sc, col, "Estado turnos Full's")
tabla_playa = _estiladorVtaTituloD3(top_20_playa, numCols, percColsPen,'Faltantes acumulados Playa: ')
tabla_full = _estiladorVtaTituloD3(top_20_full, numCols, percColsPen, 'Faltantes acumulados Full: ')




#################################turnos sin auditar playa###############################
tsa_pl  = pd.read_sql('''DECLARE @ayer DATE
                            SET @ayer = DATEADD(DAY, -1, GETDATE())
                            DECLARE @HOY DATE
                            SET @HOY = DATEADD(DAY, 0, GETDATE())
                    
					select * from 
					(select uen
						   ,FECHASQL
						   ,TURNO
                           ,iif(ESTADO = 1,'Abierto'
                            ,IIF(ESTADO = 2 and ESTAUDITORIA = 1 and NRORECA = 0,'No Auditado (No Recaudado)'
                             , iif(ESTADO = 2 and ESTAUDITORIA = 2 AND NRORECA <> 0, 'Auditado'
                              , iif (ESTADO = 2 and ESTAUDITORIA = 1 and NRORECA <> 0, 'No Auditado (Recaudado)','')))) as  'estado turno'  from VtaTurno
                    where fechasql >= '2024-01-08' and FECHASQL < @ayer 
                   ) as ttt
					where ttt.[estado turno] <> 'Auditado'
					order by FECHASQL''',db_conex)
tsa_pl['estado turno'] = tsa_pl['estado turno'].fillna('')
tsa_pl = tsa_pl.convert_dtypes()
tsa_pl['FECHASQL'] = pd.to_datetime(tsa_pl['FECHASQL'])

# Aplicar el formato deseado
tsa_pl['FECHASQL'] = tsa_pl['FECHASQL'].dt.strftime("%d/%m/%y")



#################################turnos sin auditar full###############################
tsa_full  = pd.read_sql('''DECLARE @ayer DATE
                SET @ayer = DATEADD(DAY, -1, GETDATE())
                DECLARE @HOY DATE
                SET @HOY = DATEADD(DAY, 0, GETDATE())
               select * from
			   (select uen
					  ,FECHASQL
					  ,TURNO
                       ,iif(ESTADO = 1,'Abierto'
                            ,IIF(ESTADO = 2 and AUDITADO = 0 and NRORECA <= 1,'No Auditado (No Recaudado)'
                                , iif(ESTADO = 2 and AUDITADO = 1 AND NRORECA > 1, 'Auditado'
                                    , iif (ESTADO = 2 and AUDITADO = 0 and NRORECA > 1, 'No Auditado (Recaudado)','')))) as  'estado turno' 
                       from scturnos
                        where 
                       fechasql >= '2024-10-08' and FECHASQL < @ayer 
                        and uen in ('XPRESS','AZCUENAGA','SAN JOSE','LAMADRID','PUENTE OLIVE','PERDRIEL','PERDRIEL2') and FECHASQL not in ('2024-12-01')) as ttt
						where ttt.[estado turno] <> 'Auditado' ''',db_conex)

tsa_full['estado turno'] = tsa_full['estado turno'].fillna('')
tsa_full = tsa_full.convert_dtypes()
tsa_full['FECHASQL'] = pd.to_datetime(tsa_full['FECHASQL'])

# Aplicar el formato deseado
tsa_full['FECHASQL'] = tsa_full['FECHASQL'].dt.strftime("%d/%m/%y")



####################### ESTILIZADOR tsa #############################################
def asignar_color_tsa(valor):
    if valor == 'Abierto':
        return 'color: red'
    elif valor == 'No Auditado (No Recaudado)':
        return 'color: #E3BE02'
    elif valor == 'Auditado':
        return 'color: green'
    elif valor == 'No Auditado (Recaudado)':
        return 'color: blue'
    else:
        return ''

def _estilador_tsa(df, col, titulo):
    resultado = df.style \
        .hide(axis=0) \
        .set_caption(
            titulo
            + "<br>"
            + tiempoInicio.strftime("%d/%m/%y")
            + "<br>") \
        .set_properties(subset=col, **{"text-align": "center", "width": "100px"}) \
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
                    ("background-color", "darkblue"),
                    ("color", "white")
                ]
            },
        ]) \
        .applymap(asignar_color_tsa, subset=col)  # Cambiar nombre de columna según corresponda
    evitarTotales = df.index.get_level_values(0)
    return resultado

col = ['estado turno']  # Columnas sin decimales

tsa_pl = _estilador_tsa(tsa_pl,col,"turnos playa acumulados no auditados al:") 
tsa_full = _estilador_tsa(tsa_full,col,"turnos full acumulados no auditados al:")



####################################CREACION DE IMAGENES############################################

ubicacion= str(pathlib.Path(__file__).parent)+"\\"
i_faltantes_playa = "faltantes_playa.png"
i_faltantes_full = "faltantes_full.png"
i_scturnos_hoy = "scturnos_hoy.png"
i_scturnos_ayer = "scturnos_ayer.png"
i_plturnos_hoy = "plturnos_hoy.png"
i_plturnos_ayer = "plturnos_ayer.png"
i_tsa_pl = "turnos_sin_auditar_pl.png"
i_tsa_full = "turnos_sin_auditar_full.png"

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

df_to_image(tabla_playa, ubicacion, i_faltantes_playa)
df_to_image(tabla_full, ubicacion, i_faltantes_full)
df_to_image(turnos_pl_ayer, ubicacion, i_plturnos_ayer)
df_to_image(turnos_pl_hoy, ubicacion, i_plturnos_hoy)
df_to_image(turnos_Sc, ubicacion, i_scturnos_ayer)
df_to_image(turnos_Sc_hoy, ubicacion, i_scturnos_hoy)
df_to_image(tsa_pl, ubicacion, i_tsa_pl)
df_to_image(tsa_full, ubicacion, i_tsa_full)


#############
# Timer
tiempoFinal = pd.to_datetime("today")
logger.info(
    "\nInfo Volumen de Ventas"
    + "\nTiempo de Ejecucion Total: "
    + str(tiempoFinal-tiempoInicio)
)
