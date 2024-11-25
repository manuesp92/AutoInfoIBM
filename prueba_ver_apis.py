
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

año =
#ruta="http://apis.datos.gob.ar/series/api/series/?ids=103.1_I2N_2016_M_15,101.1_I2NG_2016_M_22,102.1_I2S_ABRI_M_18,102.1_I2B_ABRI_M_15,103.1_I2R_2016_M_18,103.1_I2E_2016_M_21&format=csv"
# Definimos el dominio base y la ruta relativa
base_url = "https://www.indec.gob.ar"
relative_path = "/ftp/cuadros/economia/sh_ipc_09_24.xls"

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
        df = pd.read_excel(excel_file, sheet_name="Índices IPC Cobertura Nacional", engine='xlrd')

        # Transponemos el DataFrame (filas a columnas)
        df_transpuesta = df.T

        print('Columna transpuesta:')
        print(df_transpuesta)

        # Verificamos cuántas columnas tiene el DataFrame transpuesto
        print("Número de columnas en el DataFrame transpuesto:", df_transpuesta.shape[1])

        # Seleccionamos las filas 6 y 10 (índices 5 y 9)
        df_seleccionado = df_transpuesta.iloc[:, [5, 9]]

        # Verificamos el contenido del DataFrame seleccionado
        print("Contenido de df_seleccionado antes de renombrar columnas:")
        print(df_seleccionado)

        # Si el número de columnas es 2, asignamos los nombres de las columnas
        if df_seleccionado.shape[1] == 2:
            df_seleccionado.columns = ['Nivel general', 'Nivel general y divisiones COICOP']
        else:
            print("El número de columnas no coincide con el número de nombres de columnas.")

        # Imprimimos el resultado
        print(df_seleccionado)

    except Exception as e:
        print(f"Error al leer el archivo Excel: {e}")
else:
    print(f"Error en la solicitud: {response.status_code}")