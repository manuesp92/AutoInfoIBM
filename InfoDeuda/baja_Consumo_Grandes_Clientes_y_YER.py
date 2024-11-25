########################################################################################################################
#                                    "BAJA EN EL CONSUMO DE GRANDES CLIENTES"                                          #
########################################################################################################################

# IMPORTAMOS LOS MÓDULOS, LIBRERÍAS O PAQUETES QUE VAMOS A UTILIZAR 
import os
import math
import numpy as np
from PIL import Image
import pandas as pd
from pandas.api.types import CategoricalDtype
import pyodbc 
import sys
import pathlib
from datetime import datetime
from datetime import timedelta
from datetime import date
from datetime import timedelta,datetime
import datetime
from calendar import monthrange

# DEFINIMOS LOS VALORES PARA LA CONEXIÓN A LA BASE DE DATOS
server = "192.168.200.44\cloud"
database = "Test_Rumaos"
username = "mmagistretti" 
password = "R3dmer0s#r"

login = [server,database,username,password]

tiempoInicio = pd.to_datetime("today")

# ESTABLECEMOS LA CONEXIÓN A LA BD
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

# lE DAMOS EL FORMATO QUE DESEAMOS PARA VISUALIZAR NÚMEROS FLOTANTES
'''
pd.options.display.float_format: Este es un parámetro de configuración de pandas que define el formato en que se deben mostrar los números de punto
flotante (float). Al modificar este valor, puedes controlar cómo se ven los números decimales en las tablas.
"{:20,.0f}".format: Esta es una cadena de formato que sigue las reglas de formateo de cadenas de Python (str.format()), que controla la presentación
de los números.
20: Indica el ancho mínimo que tendrá el número al ser mostrado, es decir, el número ocupará un espacio de al menos 20 caracteres. Si el número tiene
menos caracteres, se agregarán espacios en blanco a la izquierda para rellenar.
,: Especifica que los números tendrán un separador de miles. Por ejemplo, el número 1234567 se mostrará como 1,234,567.
.0f: Indica que los números se mostrarán con 0 decimales. Es decir, se redondeará el número al entero más cercano.
'''
pd.options.display.float_format = "{:20,.0f}".format 

#################################################################
# CLIENTES CON CUENTA CORRIENTE (NO INCLUYE OPERACIONES DE YER) #
#################################################################

#GC:

# 1-CONSUMO DE LOS ÚLTIMOS 15 DÍAS POR CLIENTE
df_consumo_ultimos_15_dias= pd.read_sql("""
 SELECT 
    VCC.[NROCLIENTE],
    C.[NOMBRECLIENTE],
	SUM(VCC.[CANTIDAD]) AS Volumen_Total_ult_15_dias,
    V.[NOMBREVEND] as Nombre_Vendedor -- Nombre del vendedor asignado al cliente
    
FROM 
    [Test_Rumaos].[dbo].[H_VtaCtaCte] VCC
JOIN 
    [Test_Rumaos].[dbo].[D_Cliente_CtaCte] C
ON 
    VCC.[NROCLIENTE] = C.[NROCLIENTE]
JOIN 
    [Test_Rumaos].[dbo].[D_Vendedores] V
ON 
    C.[NROVEND] = V.[NROVEND]  -- Relaciona cliente con vendedor
WHERE 
    VCC.[FECHASQL] >= CONVERT(DATE, GETDATE() - 16) -- para tomar 15 días debo colocar 16 días para atrás desde hoy y no contemplar el día de hoy
    AND VCC.[FECHASQL] < CONVERT(DATE, GETDATE())    -- al ser menor y no menor o igual nos aseguramos de no incluir las ventas del día actual
    AND CODPRODUCTO IN ('GO','EU','NS','NU')
GROUP BY 
    VCC.[NROCLIENTE], C.[NOMBRECLIENTE], V.[NOMBREVEND]
ORDER BY 
    Volumen_Total_ult_15_dias DESC;
""", db_conex)


# 2-CONSUMO DE LOS 15 DÍAS ANTERIORES A LOS ÚLTIMOS 15 DÍAS
df_consumo_15_dias_anteriores= pd.read_sql("""
 SELECT 
    VCC.[NROCLIENTE],
    C.[NOMBRECLIENTE],
	SUM(VCC.[CANTIDAD]) AS Volumen_Total_15_dias_ant,
    V.[NOMBREVEND] Nombre_Vendedor -- Nombre del vendedor asignado al cliente
    
FROM 
    [Test_Rumaos].[dbo].[H_VtaCtaCte] VCC
JOIN 
    [Test_Rumaos].[dbo].[D_Cliente_CtaCte] C
ON 
    VCC.[NROCLIENTE] = C.[NROCLIENTE]
JOIN 
    [Test_Rumaos].[dbo].[D_Vendedores] V
ON 
    C.[NROVEND] = V.[NROVEND]  -- Relaciona cliente con vendedor
WHERE 
    VCC.[FECHASQL] >= CONVERT(DATE, GETDATE() - 31) -- para tomar 15 días debo colocar 16 días para atrás desde hoy y no contemplar el día de hoy
    AND VCC.[FECHASQL] < CONVERT(DATE, GETDATE() - 16)    -- al ser menor y no menor o igual nos aseguramos de no incluir las ventas del día actual
    AND VCC.CODPRODUCTO IN ('GO','EU','NS','NU')
GROUP BY 
    VCC.[NROCLIENTE], C.[NOMBRECLIENTE], V.[NOMBREVEND]
ORDER BY 
    Volumen_Total_15_dias_ant DESC;
""", db_conex)

# 3-CONSUMO DE LOS MEJORES 15 DÍAS DEL AÑO
df_consumo_mejores_15_dias_año= pd.read_sql("""
 WITH MonthlySales AS (
    SELECT 
        VCC.NROCLIENTE,
        C.NOMBRECLIENTE,
        YEAR(FECHASQL) AS Año,
        MONTH(FECHASQL) AS Mes,
        SUM(CANTIDAD) AS Volumen
    FROM [Test_Rumaos].[dbo].[H_VtaCtaCte] VCC
	JOIN 
    [Test_Rumaos].[dbo].[D_Cliente_CtaCte] C
	ON VCC.[NROCLIENTE] = C.[NROCLIENTE]
    WHERE CODPRODUCTO IN ('GO','EU','NS','NU')
    AND FECHASQL >= '20240101'
    GROUP BY VCC.NROCLIENTE, C.NOMBRECLIENTE, YEAR(FECHASQL), MONTH(FECHASQL)
),
RankedMonthlySales AS (
    SELECT 
        NROCLIENTE,
        NOMBRECLIENTE,
        Año,
        Mes,
        Volumen,
        ROW_NUMBER() OVER (PARTITION BY NROCLIENTE ORDER BY Volumen DESC) AS RankSales
    FROM MonthlySales
)
-- Seleccionamos solo el mes con más ventas por cliente
SELECT 
    NROCLIENTE,
    NOMBRECLIENTE,
    Mes,
    Año,
    Volumen/2 Mejor_Volumen_15_dias_del_año
FROM RankedMonthlySales
WHERE RankSales = 1;
""", db_conex)


# 4-CONSUMO PROYECTADO DEL MES
df_consumo_proyectado_mes= pd.read_sql("""
WITH Consumo_actual_mes AS (
    SELECT 
        c.NROCLIENTE,
        cl.NOMBRECLIENTE,
        SUM(c.CANTIDAD) AS consumo_actual_mes,
        DAY(GETDATE()) - 1 AS dias_transcurridos,  -- Días transcurridos hasta ayer
        DAY(EOMONTH(GETDATE())) AS dias_totales    -- Total de días del mes actual
    FROM 
        [Test_Rumaos].[dbo].[H_VtaCtaCte] c
    JOIN 
        [Test_Rumaos].[dbo].[D_Cliente_CtaCte] cl ON c.NROCLIENTE = cl.NROCLIENTE
    WHERE 
        c.FECHASQL BETWEEN CAST(DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1) AS DATE) 
        AND CAST(GETDATE() - 1 AS DATE)  -- Incluye hasta el día de ayer
		and c.CODPRODUCTO IN ('GO','EU','NS','NU')
    GROUP BY 
        c.NROCLIENTE, cl.NOMBRECLIENTE
)

    SELECT 
        NROCLIENTE,
        NOMBRECLIENTE,
        (consumo_actual_mes / dias_transcurridos) * dias_totales AS Consumo_proyectado_mes_actual
    FROM 
        Consumo_actual_mes;
""", db_conex)

# 5-MEJOR MES DE LOS ULTIMOS 12
df_consumo_mejor_mes_ult_12= pd.read_sql("""
 WITH MonthlySales AS (
    SELECT 
        VCC.NROCLIENTE,
        C.NOMBRECLIENTE,
        YEAR(FECHASQL) AS Año,
        MONTH(FECHASQL) AS Mes,
        SUM(CANTIDAD) AS Mejor_Volumen
    FROM [Test_Rumaos].[dbo].[H_VtaCtaCte] VCC
	JOIN [Test_Rumaos].[dbo].[D_Cliente_CtaCte] C
	ON VCC.[NROCLIENTE] = C.[NROCLIENTE]
    WHERE CODPRODUCTO IN ('GO','EU','NS','NU')
    AND FECHASQL >= DATEADD(MONTH, -12, GETDATE()) -- Filtra los últimos 12 meses
    GROUP BY VCC.NROCLIENTE, C.NOMBRECLIENTE, YEAR(FECHASQL), MONTH(FECHASQL)
),
RankedMonthlySales AS (
    SELECT 
        NROCLIENTE,
        NOMBRECLIENTE,
        Año,
        Mes,
        Mejor_Volumen,
        ROW_NUMBER() OVER (PARTITION BY NROCLIENTE ORDER BY Mejor_Volumen DESC) AS RankSales
    FROM MonthlySales
)
-- Seleccionamos solo el mes con más ventas por cliente
SELECT 
    NROCLIENTE,
    NOMBRECLIENTE,
    Mes,
    Año,
    Mejor_Volumen Mejor_Volumen_ult_12_meses
FROM RankedMonthlySales
WHERE RankSales = 1;
""", db_conex)

#YER:

# 1-CONSUMO DE LOS ÚLTIMOS 15 DÍAS POR CLIENTE
df_consumo_ultimos_15_dias_YER= pd.read_sql("""
SELECT 
    NOMBRECLIENTE,
    SUM(VOLDESP) AS Volumen_Total_ult_15_dias,
    NOMBREVEND as Nombre_Vendedor -- Nombre del vendedor asignado al cliente
FROM [Test_Rumaos].[dbo].[H_VentasYER]
WHERE 
    [FECHASQL] >= CONVERT(DATE, GETDATE() - 16) -- para tomar 15 días debo colocar 16 días para atrás desde hoy y no contemplar el día de hoy
    AND [FECHASQL] < CONVERT(DATE, GETDATE())    -- al ser menor y no menor o igual nos aseguramos de no incluir las ventas del día actual
    AND CODPRODUCTO IN ('GO','EU','NS','NU')
	GROUP BY 
    NOMBRECLIENTE, NOMBREVEND
ORDER BY 
    Volumen_Total_ult_15_dias DESC;
""", db_conex)

# 2-CONSUMO DE LOS 15 DÍAS ANTERIORES A LOS ÚLTIMOS 15 DÍAS
df_consumo_15_dias_anteriores_YER= pd.read_sql("""
SELECT 
    NOMBRECLIENTE,
    SUM(VOLDESP) AS Volumen_Total_15_dias_ant,
    NOMBREVEND as Nombre_Vendedor -- Nombre del vendedor asignado al cliente
FROM [Test_Rumaos].[dbo].[H_VentasYER]
WHERE 
    [FECHASQL] >= CONVERT(DATE, GETDATE() - 31) -- para tomar 15 días debo colocar 16 días para atrás desde hoy y no contemplar el día de hoy
    AND [FECHASQL] < CONVERT(DATE, GETDATE() - 16)    -- al ser menor y no menor o igual nos aseguramos de no incluir las ventas del día actual
    AND CODPRODUCTO IN ('GO','EU','NS','NU')
	GROUP BY 
    NOMBRECLIENTE, NOMBREVEND
ORDER BY 
    Volumen_Total_15_dias_ant DESC;
""", db_conex)

# 3-CONSUMO DE LOS MEJORES 15 DÍAS DEL AÑO
df_consumo_mejores_15_dias_año_YER= pd.read_sql("""
 WITH MonthlySales AS (
    SELECT 
        NOMBRECLIENTE,
        YEAR(FECHASQL) AS Año,
        MONTH(FECHASQL) AS Mes,
        SUM(VOLDESP) AS Volumen
    FROM [Test_Rumaos].[dbo].[H_VentasYER]
	WHERE CODPRODUCTO IN ('GO','EU','NS','NU')
    AND FECHASQL >= '20240101'
    GROUP BY NOMBRECLIENTE, YEAR(FECHASQL), MONTH(FECHASQL)
),
RankedMonthlySales AS (
    SELECT 
        NOMBRECLIENTE,
        Año,
        Mes,
        Volumen,
        ROW_NUMBER() OVER (PARTITION BY NOMBRECLIENTE ORDER BY Volumen DESC) AS RankSales
    FROM MonthlySales
)
-- Seleccionamos solo el mes con más ventas por cliente
SELECT 
    NOMBRECLIENTE,
    Mes,
    Año,
    Volumen/2 Mejor_Volumen_15_dias_del_año
FROM RankedMonthlySales
WHERE RankSales = 1;
""", db_conex)

# 4-CONSUMO PROYECTADO DEL MES
df_consumo_proyectado_mes_YER= pd.read_sql("""
WITH Consumo_actual_mes AS (
    SELECT 
        NOMBRECLIENTE,
        SUM(VOLDESP) AS consumo_actual_mes,
        DAY(GETDATE()) - 1 AS dias_transcurridos,  -- Días transcurridos hasta ayer
        DAY(EOMONTH(GETDATE())) AS dias_totales    -- Total de días del mes actual
    FROM [Test_Rumaos].[dbo].[H_VentasYER]
    WHERE 
        FECHASQL BETWEEN CAST(DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1) AS DATE) 
        AND CAST(GETDATE() - 1 AS DATE)  -- Incluye hasta el día de ayer
		and CODPRODUCTO IN ('GO','EU','NS','NU')
    GROUP BY 
        NOMBRECLIENTE
)

    SELECT 
         NOMBRECLIENTE,
        (consumo_actual_mes / dias_transcurridos) * dias_totales AS Consumo_proyectado_mes_actual
    FROM 
        Consumo_actual_mes;
""", db_conex)

# 5-MEJOR MES DE LOS ULTIMOS 12
df_consumo_mejor_mes_ult_12_YER= pd.read_sql("""
 WITH MonthlySales AS (
    SELECT 
        NOMBRECLIENTE,
        YEAR(FECHASQL) AS Año,
        MONTH(FECHASQL) AS Mes,
        SUM(VOLDESP) AS Volumen
    FROM [Test_Rumaos].[dbo].[H_VentasYER]
    WHERE CODPRODUCTO IN ('GO','EU','NS','NU')
    AND FECHASQL >= DATEADD(MONTH, -12, GETDATE()) -- Filtra los últimos 12 meses
    GROUP BY NOMBRECLIENTE, YEAR(FECHASQL), MONTH(FECHASQL)
),
RankedMonthlySales AS (
    SELECT 
        NOMBRECLIENTE,
        Año,
        Mes,
        Volumen,
        ROW_NUMBER() OVER (PARTITION BY NOMBRECLIENTE ORDER BY Volumen DESC) AS RankSales
    FROM MonthlySales
)
-- Seleccionamos solo el mes con más ventas por cliente
SELECT 
    NOMBRECLIENTE,
    Mes,
    Año,
    Volumen Mejor_Volumen_ult_12_meses
FROM RankedMonthlySales
WHERE RankSales = 1;
""", db_conex)

#Excel GC:
# Realizamos los merges con base en NROCLIENTE y NOMBRECLIENTE, utilizando outer join para incluir todos los clientes

# Merge de los primeros 2 DataFrames (últimos 15 días y los 15 días anteriores)
df_final_gc = pd.merge(df_consumo_ultimos_15_dias, 
                    df_consumo_15_dias_anteriores, 
                    on=["NROCLIENTE", "NOMBRECLIENTE", "Nombre_Vendedor"], 
                    how="outer",  # Cambiamos a outer para incluir todos los registros
                    suffixes=('_ult_15_dias', '_15_dias_ant'))

# Merge con el consumo de los mejores 15 días del año
df_final_gc = pd.merge(df_final_gc, 
                    df_consumo_mejores_15_dias_año[['NROCLIENTE', 'NOMBRECLIENTE', 'Mejor_Volumen_15_dias_del_año']], 
                    on=["NROCLIENTE", "NOMBRECLIENTE"], 
                    how="outer")  # Cambiamos a outer

# Merge con el consumo proyectado del mes actual
df_final_gc = pd.merge(df_final_gc, 
                    df_consumo_proyectado_mes[['NROCLIENTE', 'NOMBRECLIENTE', 'Consumo_proyectado_mes_actual']], 
                    on=["NROCLIENTE", "NOMBRECLIENTE"], 
                    how="outer")  # Cambiamos a outer

# Merge con el mejor volumen de los últimos 12 meses
df_final_gc = pd.merge(df_final_gc, 
                    df_consumo_mejor_mes_ult_12[['NROCLIENTE', 'NOMBRECLIENTE', 'Mejor_Volumen_ult_12_meses']], 
                    on=["NROCLIENTE", "NOMBRECLIENTE"], 
                    how="outer")  # Cambiamos a outer

# Calculamos las nuevas columnas
df_final_gc['Dif_ult_15_dias_vs_15_dias_previos'] = df_final_gc['Volumen_Total_ult_15_dias'] - df_final_gc['Volumen_Total_15_dias_ant']
df_final_gc['Dif_ult_15_dias_vs_mejor_volumen_año'] = df_final_gc['Volumen_Total_ult_15_dias'] - df_final_gc['Mejor_Volumen_15_dias_del_año']
df_final_gc['Dif_ult_15_dias_vs_mejor_volumen_ult_12_meses'] = df_final_gc['Volumen_Total_ult_15_dias'] - df_final_gc['Mejor_Volumen_ult_12_meses']

# Reemplazar NaN por 0 en las columnas numéricas
# Seleccionamos solo las columnas numéricas para rellenar con 0
num_cols = df_final_gc.select_dtypes(include='number').columns
df_final_gc[num_cols] = df_final_gc[num_cols].fillna(0)

# Reorganizamos las columnas para que 'Nombre_Vendedor' esté antes de 'Volumen_Total_ult_15_dias'
column_order_gc = ['NROCLIENTE', 'NOMBRECLIENTE', 'Nombre_Vendedor', 'Volumen_Total_ult_15_dias', 
                'Volumen_Total_15_dias_ant', 'Dif_ult_15_dias_vs_15_dias_previos' ,'Mejor_Volumen_15_dias_del_año','Dif_ult_15_dias_vs_mejor_volumen_año', 
                'Mejor_Volumen_ult_12_meses','Dif_ult_15_dias_vs_mejor_volumen_ult_12_meses','Consumo_proyectado_mes_actual']

# Asegúrate de que las columnas existan antes de reorganizar
df_final_gc = df_final_gc[[col for col in column_order_gc if col in df_final_gc.columns]]

#Excel YER:
# Realizamos los merges con base en NOMBRECLIENTE, utilizando outer join para incluir todos los clientes

# Merge de los primeros 2 DataFrames (últimos 15 días y los 15 días anteriores)
df_final_YER = pd.merge(df_consumo_ultimos_15_dias_YER, 
                    df_consumo_15_dias_anteriores_YER, 
                    on=["NOMBRECLIENTE", "Nombre_Vendedor"], 
                    how="outer",  # Cambiamos a outer para incluir todos los registros
                    suffixes=('_ult_15_dias', '_15_dias_ant'))

# Merge con el consumo de los mejores 15 días del año
df_final_YER = pd.merge(df_final_YER, 
                    df_consumo_mejores_15_dias_año_YER[['NOMBRECLIENTE', 'Mejor_Volumen_15_dias_del_año']], 
                    on=["NOMBRECLIENTE"], 
                    how="outer")  # Cambiamos a outer

# Merge con el consumo proyectado del mes actual
df_final_YER = pd.merge(df_final_YER, 
                    df_consumo_proyectado_mes_YER[['NOMBRECLIENTE', 'Consumo_proyectado_mes_actual']], 
                    on=["NOMBRECLIENTE"], 
                    how="outer")  # Cambiamos a outer

# Merge con el mejor volumen de los últimos 12 meses
df_final_YER = pd.merge(df_final_YER, 
                    df_consumo_mejor_mes_ult_12_YER[['NOMBRECLIENTE', 'Mejor_Volumen_ult_12_meses']], 
                    on=["NOMBRECLIENTE"], 
                    how="outer")  # Cambiamos a outer

# Calculamos las nuevas columnas
df_final_YER['Dif_ult_15_dias_vs_15_dias_previos'] = df_final_YER['Volumen_Total_ult_15_dias'] - df_final_YER['Volumen_Total_15_dias_ant']
df_final_YER['Dif_ult_15_dias_vs_mejor_volumen_año'] = df_final_YER['Volumen_Total_ult_15_dias'] - df_final_YER['Mejor_Volumen_15_dias_del_año']
df_final_YER['Dif_ult_15_dias_vs_mejor_volumen_ult_12_meses'] = df_final_YER['Volumen_Total_ult_15_dias'] - df_final_YER['Mejor_Volumen_ult_12_meses']

# Reemplazar NaN por 0 en las columnas numéricas
# Seleccionamos solo las columnas numéricas para rellenar con 0
num_cols = df_final_YER.select_dtypes(include='number').columns
df_final_YER[num_cols] = df_final_YER[num_cols].fillna(0)

# Reorganizamos las columnas para que 'Nombre_Vendedor' esté antes de 'Volumen_Total_ult_15_dias'
column_order_YER = ['NROCLIENTE', 'NOMBRECLIENTE', 'Nombre_Vendedor', 'Volumen_Total_ult_15_dias', 
                'Volumen_Total_15_dias_ant', 'Dif_ult_15_dias_vs_15_dias_previos' ,'Mejor_Volumen_15_dias_del_año','Dif_ult_15_dias_vs_mejor_volumen_año', 
                'Mejor_Volumen_ult_12_meses','Dif_ult_15_dias_vs_mejor_volumen_ult_12_meses','Consumo_proyectado_mes_actual']

# Asegúrate de que las columnas existan antes de reorganizar
df_final_YER = df_final_YER[[col for col in column_order_YER if col in df_final_YER.columns]]

# Guardamos ambos DataFrames en un archivo Excel
ubicacion = str(pathlib.Path(__file__).parent) + "\\"
nombreExcel = "Consumo_GC_YER.xlsx"
excel_path = ubicacion + nombreExcel

# Creamos el archivo Excel
with pd.ExcelWriter(excel_path, engine='xlsxwriter') as writer:
    df_final_gc.to_excel(writer, sheet_name="ConsumoGC", index=False, na_rep="")
    
    df_final_YER.to_excel(writer, sheet_name="ConsumoYER", index=False, na_rep="")

     # Aplicar formato condicional a las columnas relevantes
    format_neg = writer.book.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})  # Rojo para negativos
    format_pos = writer.book.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100'})  # Verde para positivos
    
    # Accedemos al libro y hoja de trabajo para ajustar el ancho de las columnas de GC
    worksheet_gc = writer.sheets["ConsumoGC"]
    for idx, col in enumerate(df_final_gc.columns):
        max_len = max(df_final_gc[col].astype(str).map(len).max(), len(col)) + 2  # Añadimos espacio extra
        worksheet_gc.set_column(idx, idx, max_len)  # Ajustamos el ancho de la columna
    
    # List of columns to apply conditional formatting in GC
    columns_gc = ['Dif_ult_15_dias_vs_15_dias_previos', 'Dif_ult_15_dias_vs_mejor_volumen_año', 'Dif_ult_15_dias_vs_mejor_volumen_ult_12_meses']
    for col in columns_gc:
        col_idx = df_final_gc.columns.get_loc(col)  # Get column index
        worksheet_gc.conditional_format(1, col_idx, len(df_final_gc), col_idx, 
                                        {'type': 'cell', 'criteria': '<', 'value': 0, 'format': format_neg})
        worksheet_gc.conditional_format(1, col_idx, len(df_final_gc), col_idx, 
                                        {'type': 'cell', 'criteria': '>', 'value': 0, 'format': format_pos})
    
    # Accedemos al libro y hoja de trabajo para ajustar el ancho de las columnas de YER
    worksheet_yer = writer.sheets["ConsumoYER"]
    for idx, col in enumerate(df_final_YER.columns):
        max_len = max(df_final_YER[col].astype(str).map(len).max(), len(col)) + 2
        worksheet_yer.set_column(idx, idx, max_len)
    
    # Aplicar formato condicional a las columnas relevantes de ConsumoYER
    for col in columns_gc:
        if col in df_final_YER.columns:
            col_idx = df_final_YER.columns.get_loc(col)  # Get column index
            worksheet_yer.conditional_format(1, col_idx, len(df_final_YER), col_idx, 
                                             {'type': 'cell', 'criteria': '<', 'value': 0, 'format': format_neg})
            worksheet_yer.conditional_format(1, col_idx, len(df_final_YER), col_idx, 
                                             {'type': 'cell', 'criteria': '>', 'value': 0, 'format': format_pos})

print(f"Archivo Excel creado en: {excel_path}")
