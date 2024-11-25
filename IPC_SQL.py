
import requests
import time
import pandas as pd
import json
from oauth2client.service_account import ServiceAccountCredentials
import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import pyodbc
from io import StringIO
from io import BytesIO  # Usar BytesIO para manejar archivos binarios como Excel
from datetime import datetime, timedelta


# Obtener la fecha actual
fecha_actual = datetime.now()


# Lógica para el mes
if fecha_actual.month == 1 and fecha_actual.day <= 15:
    # Si es enero y el día es 15 o antes, devolver diciembre
    mes = '12'  # Diciembre en formato numérico str
else:
    if fecha_actual.day > 16:
        # Si es después del día 16, devolver el mes actual
        mes = fecha_actual.strftime('%m')  # Obtiene el mes actual en formato numérico
    else:
        # Si es el 16 o antes, devolver el mes anterior
        mes_anterior = fecha_actual - timedelta(days=fecha_actual.day)  # Resta el número de días para ir al mes anterior
        mes = mes_anterior.strftime('%m')  # Obtiene el mes anterior en formato numérico

# Lógica para el anio
if fecha_actual.month == 1 and fecha_actual.day < 16:
    # Si es antes del 16 de enero, devolver el año anterior
    anio = (fecha_actual.year - 1) % 100  # Obtiene el anio anterior en formato 2 dígitos
else:
    # Devolver el anio actual en formato 2 dígitos
    anio = fecha_actual.year % 100  # Obtiene el anio actual en formato 2 dígitos


#ruta="http://apis.datos.gob.ar/series/api/series/?ids=103.1_I2N_2016_M_15,101.1_I2NG_2016_M_22,102.1_I2S_ABRI_M_18,102.1_I2B_ABRI_M_15,103.1_I2R_2016_M_18,103.1_I2E_2016_M_21&format=csv"
# Definimos el dominio base y la ruta relativa
base_url = "https://www.indec.gob.ar"
relative_path = "/ftp/cuadros/economia/sh_ipc_"+str(mes)+"_"+str(anio)+".xls"

# Creamos la URL completa
ruta = base_url + relative_path
# Hacemos una petición HTTP a la API de bluelytics
response = requests.get(ruta, timeout=60)

# Si la petición fue exitosa, obtenemos la respuesta
if response.status_code == 200:
    
    # Utiliza StringIO para leer csv o BytesIO para excel, para convertir la cadena en un archivo "virtual"
    #csv_file = StringIO(response.content)
    
    excel_file = BytesIO(response.content)
    
    # Intentamos leer la hoja específica con pandas
    try:
        # Leer la hoja "Índices IPC Cobertura Nacional"
        df = pd.read_excel(excel_file, sheet_name="Índices IPC Cobertura Nacional", engine='xlrd')

        # Seleccionamos las filas 6 y 10 
        df_seleccionado = df.iloc[[4, 8]]

        # Transponemos el DataFrame (filas a columnas)
        df_transpuesta = df_seleccionado.T
        
        #Cambiamos nombres de columnas:
        df_transpuesta_renom = df_transpuesta.rename(columns={df_transpuesta.columns[0]: 'indice_tiempo', df_transpuesta.columns[1]: 'IPC'})

        # Eliminar la primera fila
        df_transpuesta_renom = df_transpuesta_renom.iloc[1:] # Seleccionar la fila con índice 0
        
    except Exception as e:
        print(f"Error al leer el archivo Excel: {e}")

else:
    print(f"Error en la solicitud: {response.status_code}")

# Cambiamos la columna mes a tipo datetime
df_transpuesta_renom['indice_tiempo'] = pd.to_datetime(df_transpuesta_renom['indice_tiempo'])

# Convertir la columna 'indice_tiempo' a string
df_transpuesta_renom['indice_tiempo'] = df_transpuesta_renom['indice_tiempo'].dt.strftime('%Y-%m-%d') 

# Convertir la columna 'IPC' a tipo numérico (si es necesario)
df_transpuesta_renom['IPC'] = pd.to_numeric(df_transpuesta_renom['IPC'], errors='coerce')

# Calcular ipc_2016_nivel_general
df_transpuesta_renom['ipc_2016_nivel_general'] = (df_transpuesta_renom['IPC'] + 1) / df_transpuesta_renom['IPC'].shift(1) - 1

# Reemplazar NaN en la nueva columna con None
df_transpuesta_renom['ipc_2016_nivel_general'] = df_transpuesta_renom['ipc_2016_nivel_general'].where(df_transpuesta_renom['ipc_2016_nivel_general'].notna(), 0)

# Intercambiar el contenido de las columnas IPC e ipc_2016_nivel_general
df_transpuesta_renom['IPC'], df_transpuesta_renom['ipc_2016_nivel_general'] = df_transpuesta_renom['ipc_2016_nivel_general'].copy(), df_transpuesta_renom['IPC'].copy()

# Establecer la conexión con la base de datos SQL Server
server = "192.168.200.44\cloud"
database = "Test_Rumaos"
username = "itiersoper01" 
password = "redMerco1234#"

login = [server, database, username, password]

conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=" + login[0] + ";"
    "DATABASE=" + login[1] + ";"
    "UID=" + login[2] + ";"
    "PWD=" + login[3]
)

# Nombre de la tabla en SQL Server
table_name = "D_IPC"

# Generar la sentencia SQL INSERT
columns = df_transpuesta_renom.columns.tolist()
columns_str = ', '.join(columns)
placeholders = ', '.join(['?' for _ in range(len(columns))])
insert_query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders});"

cursor = conn.cursor()

# Borrar los datos en la tabla
delete_query = f"DELETE FROM {table_name};"
cursor.execute(delete_query)
conn.commit()

# Cargar los datos en la tabla
cursor.fast_executemany = True
try:
    cursor.executemany(insert_query, df_transpuesta_renom.values.tolist())
    conn.commit()
except Exception as e:
    print(f"Error al insertar los datos: {e}")

# Cerrar la conexión con la base de datos
conn.close()

print("CARGA COMPLETA")