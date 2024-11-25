
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


scope= ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
rutaJson='https://api.bluelytics.com.ar/v2/evolution.json'
ruta2="C:/Users/mespinosa/AppData/Local/Microsoft/WindowsApps/Informes/dolar-438211-6f1d8a477a84.json"

def cargar(df):

    credenciales = ServiceAccountCredentials.from_json_keyfile_name(ruta2, scope)
    cliente = gspread.authorize(credenciales)
    sheet = cliente.open_by_key("1Hl-wltOWpYpMHwFBNSheBWNSuN4rKHgYxEB3WlycqgI").worksheet('IPC')  
    sheet.clear()
    sheet.append_rows([df.columns.values.tolist()]+ df.values.tolist())
    print('Carga completa')


# Obtener la fecha actual
fecha_actual = datetime.now()


# Lógica para el mes
if fecha_actual.day > 16:
    # Si es después del día 15, devolver el mes actual
    mes = fecha_actual.strftime('%m')  # Obtiene el mes actual en formato numérico
else:
    # Si es el 15 o antes, devolver el mes anterior
    mes_anterior = fecha_actual - timedelta(days=fecha_actual.day)  # Resta el número de días para ir al mes anterior
    mes = mes_anterior.strftime('%m')  # Obtiene el mes anterior en formato numérico

# Lógica para el anio
if fecha_actual.month == 1 and fecha_actual.day < 16:
    # Si es antes del 16 de enero, devolver el año anterior
    anio = (fecha_actual.year - 1) % 100  # Obtiene el anio anterior en formato 2 dígitos
else:
    # Devolver el anio actual en formato 2 dígitos
    anio = fecha_actual.year % 100  # Obtiene el anio actual en formato 2 dígitos

mes_str = str(mes)
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
        df_transpuesta_renom = df_transpuesta.rename(columns={df_transpuesta.columns[0]: 'Mes', df_transpuesta.columns[1]: 'IPC'})

        # Eliminar la primera fila
        df_transpuesta_renom = df_transpuesta_renom.iloc[1:] #Solo seleccionamos a partir de la 2da fila (es decir a partir del indice 1)
        
    except Exception as e:
        print(f"Error al leer el archivo Excel: {e}")

else:
    print(f"Error en la solicitud: {response.status_code}")

# Cambiamos la columna mes a tipo datetime
df_transpuesta_renom['Mes'] = pd.to_datetime(df_transpuesta_renom['Mes'])

# Convertir la columna 'Mes' a string
df_transpuesta_renom['Mes'] = df_transpuesta_renom['Mes'].dt.strftime('%Y-%m-%d') 

# Cargamos los datos en el nuevo Google Sheet
cargar(df_transpuesta_renom)

'''
print(archivo.dtypes)
print(archivo)
# Establecer la conexión con la base de datos SQL Server
server = "192.168.200.44\cloud"
database = "Test_Rumaos"
username = "itiersoper01" 
password = "redMerco1234#"

login = [server,database,username,password]
#################################

tiempoInicio = pd.to_datetime("today")
#########
# Formating display of dataframes with comma separator for numbers
pd.options.display.float_format = "{:20,.0f}".format 
#########
conn = pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};\
        SERVER="+login[0]+";\
        DATABASE="+login[1]+";\
        UID="+login[2]+";\
        PWD="+ login[3]
    )

    
    
# Nombre de la tabla en SQL Server
table_name = "D_IPC"

# Generar la sentencia SQL INSERT
columns = archivo.columns.tolist()
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
cursor.executemany(insert_query, archivo.values.tolist())
conn.commit()

# Cerrar la conexión con la base de datos
conn.close()

print("CARGA COMPLETA")
'''