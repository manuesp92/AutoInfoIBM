
import requests
import time
import pandas as pd
import json
from oauth2client.service_account import ServiceAccountCredentials
import gspread

scope= ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
ruta="C:/Users/mespinosa/AppData/Local/Microsoft/WindowsApps/Informes/dolar-438211-6f1d8a477a84.json"
rutaJson='https://api.bluelytics.com.ar/v2/evolution.json'

def cargar(df):
    credenciales = ServiceAccountCredentials.from_json_keyfile_name(ruta, scope)
    cliente = gspread.authorize(credenciales)
    sheet = cliente.open_by_key("1Hl-wltOWpYpMHwFBNSheBWNSuN4rKHgYxEB3WlycqgI").worksheet('Dolar')  # Reemplaza con tu ID
    sheet.clear()
    df['date'] = df['date'].dt.strftime('%Y-%m-%d')

    sheet.append_rows([df.columns.values.tolist()]+ df.values.tolist())
    print('Carga completa')

# Hacemos una petición HTTP a la API de Bluelytics
response = requests.get(rutaJson, timeout=60)

# Si la petición fue exitosa (código 200)
if response.status_code == 200:
    # Convertimos la respuesta a formato JSON
    response_json = response.json()
    
    # Convertimos el JSON a un DataFrame
    archivo = pd.DataFrame(response_json)
    archivo['date'] = pd.to_datetime(archivo['date'])

    # Creamos un rango de fechas completo
    date_range = pd.date_range(start=archivo['date'].min(), end=archivo['date'].max(), freq='D')
    all_dates_df = pd.DataFrame({'date': date_range})
    
    # Ordenamos los datos por fecha
    archivo = archivo.sort_values(by='date')
    
    # Filtramos para obtener solo los datos del dólar "Blue"
    archivo = archivo.loc[archivo['source'] == 'Blue']
    archivo = archivo.drop(columns=['source'])
    
    # Combinamos los datos del archivo con las fechas completas
    df_new = pd.merge_asof(all_dates_df, archivo, on='date')
    
    # Cargamos los datos en el nuevo Google Sheet
    cargar(df_new)


        
        

