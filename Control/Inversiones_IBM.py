import pandas as pd
import os
import pathlib
import sys
import dataframe_image as dfi
from datetime import datetime
#sys.path.insert(0,str(pathlib.Path(__file__).parent.parent))

tiempoInicio = pd.to_datetime("today")

sheet_id = "1Sx-TrbQbWxlBdjYP5Tr6WkxBL9lnQcpN9TRl6vS8EhI"
def get_sheet(sheet_name, sheet_id):
    gsheet_url_costo = "https://docs.google.com/spreadsheets/d/{}/gviz/tq?tqx=out:csv&sheet={}".format(sheet_id, sheet_name)
    costo=pd.read_csv(gsheet_url_costo)
    costo= costo.convert_dtypes()
    return costo

bonos = get_sheet("bono", sheet_id)

capital = get_sheet("Capital", sheet_id)
capital['Stand By'] = capital['Stand By'].astype(pd.StringDtype())
#print(capital.info())
#print(capital)

for column in ['Capital Liquido', 'Capital Invertido', 'Capital Total', 'Stand By']:
    
    # Eliminar el separador de miles (puntos)
    capital[column] = capital[column].str.replace('.', '', regex=False)
    
    # Reemplazar la coma por punto para manejar los decimales
    capital[column] = capital[column].str.replace(',', '.', regex=False)
    
    # Convertir la columna a numérico
    capital[column] = pd.to_numeric(capital[column], errors='coerce')
# Verificar los tipos de datos para asegurar que la conversión fue correcta
#print(capital.info())
#print(capital)

operaciones = get_sheet("Operaciones2", sheet_id)

columns_to_replace2 = ['Ganancia Total %']

# Reemplaza las comas por puntos en las columnas especificadas
for col in columns_to_replace2:
    try:
        operaciones[col] = operaciones[col].str.replace(',', '.')
    except:
        print('ole')
operaciones['Ganancia Total %'] = operaciones['Ganancia Total %'].astype(float)


cartera = get_sheet("InversionesIBM", sheet_id)
cartera['Fecha de Compra'] = pd.to_datetime(cartera['Fecha de Compra'], format='%d/%m/%Y')
cartera = cartera.fillna('0')
# Especifica las columnas en las que deseas reemplazar las comas por puntos
columns_to_replace = ['Precio de Compra', 'Precio Actual','Ganancia Total %','Ganancia Total $','Precio de Apertura',
                     'Ganancia Diaria','Ganancia Diaria %','Valor Bursatil','Precio Objetivo','Ganancia potencial']

# Reemplaza las comas por puntos en las columnas especificadas
for col in columns_to_replace:
    try:
        cartera[col] = cartera[col].str.replace(',', '.')
    except:
        print('ole')

    
hoy = datetime.now()

# Calcular la diferencia en días y crear una nueva columna
cartera['Dias transcurridos'] = (hoy - cartera['Fecha de Compra']).dt.days

cartera['Precio de Compra'] = cartera['Precio de Compra'].astype(float)
cartera['Precio Actual'] = cartera['Precio Actual'].astype(float)
cartera['Ganancia Total %'] = cartera['Ganancia Total %'].astype(float)
cartera['Ganancia Total $'] = cartera['Ganancia Total $'].astype(float)
cartera['Precio de Apertura'] = cartera['Precio de Apertura'].astype(float)
cartera['Ganancia Diaria'] = cartera['Ganancia Diaria'].astype(float)
cartera['Ganancia Diaria %'] = cartera['Ganancia Diaria %'].astype(float)
cartera['Valor Bursatil'] = cartera['Valor Bursatil'].astype(float)
cartera['Precio Objetivo'] = cartera['Precio Objetivo'].astype(float)
cartera['Ganancia potencial'] = cartera['Ganancia potencial'].astype(float)

cartera['Inversion Inicial']= cartera['Cantidad']*cartera['Precio de Compra']
cartera['Inversion total apertura']= cartera['Cantidad']*cartera['Precio de Apertura']
######### COSTOS DAPSA EU

cartera.loc["colTOTAL"]= pd.Series(
    cartera.sum(numeric_only=True)
    , index=["Ganancia Total $","Ganancia Diaria","Valor Bursatil",'Inversion Inicial','Inversion total apertura','Ganancia potencial']
)
# Fill NaN in UEN column at total row
cartera.fillna({"Simbolo":"TOTAL"}, inplace=True)



tasa = ((cartera.loc["colTOTAL","Valor Bursatil"]-cartera.loc["colTOTAL","Inversion Inicial"]) /
    cartera.loc["colTOTAL","Inversion Inicial"])
cartera.fillna({"Ganancia Total %":tasa}, inplace=True)

tasa2 = ((cartera.loc["colTOTAL","Valor Bursatil"]-cartera.loc["colTOTAL","Inversion total apertura"]) /
    cartera.loc["colTOTAL","Inversion total apertura"])
cartera.fillna({"Ganancia Diaria %":tasa2}, inplace=True)

cartera['Fecha de Compra'] = cartera['Fecha de Compra'].fillna('')
cartera = cartera.fillna(0)
cartera['% para Alcanzar Stop Profit']= (cartera['Precio Objetivo'] / cartera['Precio Actual'])-1

cartera2 = cartera.reindex(columns=['Simbolo','Precio Actual','Precio Objetivo','% para Alcanzar Stop Profit','Ganancia potencial'
                          ,'Valor Bursatil','Dias transcurridos'])
cartera = cartera.reindex(columns=['Simbolo','Cantidad','Precio Actual','Valor Bursatil',
                          'Ganancia Diaria','Ganancia Diaria %','Ganancia Total $','Ganancia Total %'])

bonos = bonos.rename(columns={'Unnamed: 1':'Especie'})

bonos = bonos[bonos['Especie'].isin(['GD29', 'GD30','GD35','GD38','GD41','GD46'])]
bonos = bonos.drop_duplicates(subset='Especie', keep='first')
bonos = bonos.reindex(columns=['Especie','Paridad','Px USD','% Día'])
bonos['% Día'] = bonos['% Día'].str.replace('%', '').str.replace(',', '.').astype(float) / 100

def _estilador(df,list_Col_Num, list_Col_Num0,list_Col_Perc,list_Col_Date,list_Col_pesos, titulo, evitarTotal):
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
    df_temp = df.iloc[:-1]
    resultado = df.style \
        .format("{0:,.0f}", subset=list_Col_Num0) \
        .format("${0:,.2f}", subset=list_Col_Num) \
        .format("{:,.2%}", subset=list_Col_Perc) \
        .format("${:,.0f}", subset=list_Col_pesos) \
        .format(lambda x: x.strftime("%d/%m/%Y"), subset=pd.IndexSlice[df_temp.index, list_Col_Date]) \
        .hide(axis=0) \
        .set_caption(
            titulo
            + "<br>"
            + ((tiempoInicio-pd.to_timedelta(0,"days")).strftime("%d/%m/%y"))
            + "<br>") \
        .set_properties(subset= list_Col_Perc + list_Col_Num + list_Col_Num0
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
        .apply(lambda x: ["background: white" if x.name == "colTOTAL" 
            else "" for i in x]
            , axis=1) \
        .apply(lambda x: [color_total(i) if x.name == "colTOTAL"
                          else "" for i in x], axis=1)
    
    evitarTotales = df.index.get_level_values(0) 
    if evitarTotal==1:
        subset_columns = pd.IndexSlice[evitarTotales[:-1],list_Col_Perc]
    else:
        subset_columns = pd.IndexSlice[list_Col_Perc]

        

    resultado= resultado.applymap(table_color,subset=subset_columns)
    
    return resultado

def table_color(val):
    """
    Takes a scalar and returns a string with
    the css property `'color: red'` for less than 60 marks, green otherwise.
    """
    if pd.notnull(val) and val > 0.0:
        color = 'blue'
    elif pd.isna(val) or str(val).lower() == 'nan%':
        color = 'white'
    else:
        color = 'red'
    return 'color: % s' % color

def color_total(val):
    # Función para aplicar color condicionalmente
    if val == 0 or str(val).lower() == 'nan%'  :
        return 'color: white;'
    return 'color: black;'

##Columnas sin decimales
numCols0 = ['Cantidad']
numCols1=['Precio Actual','Ganancia Total $','Ganancia Diaria','Valor Bursatil']
list_Col_Date = []
percColsPen = ['Ganancia Total %','Ganancia Diaria %']
list_Col_pesos=[]
cartera_estilado = _estilador(cartera, numCols1, numCols0, percColsPen,list_Col_Date,list_Col_pesos, 'Posiciones Activa Portfolio',0)


numCols02 = ['Dias transcurridos']
numCols12=['Valor Bursatil','Ganancia potencial','Precio Objetivo','Precio Actual']
list_Col_Date2 = []
percColsPen2 = ['% para Alcanzar Stop Profit']
list_Col_pesos2=[]
cartera2_estilado = _estilador(cartera2, numCols12, numCols02, percColsPen2,list_Col_Date2,list_Col_pesos2, 'Posiciones Activas Objetivo Portfolio',0)


numCols03 = []
numCols13=[]
list_Col_Date3 = []
percColsPen3 = []
list_Col_pesos3=['Capital Liquido','Capital Invertido','Capital Total','Stand By']
capital_estilado = _estilador(capital, numCols13, numCols03, percColsPen3,list_Col_Date3,list_Col_pesos3, 'Capital',0)


numCols04 = []
numCols14=[]
list_Col_Date4 = []
percColsPen4 = ['Ganancia Total %']
list_Col_pesos4=['Ganancia Total $','Capital Invertido']
operaciones_estilado = _estilador(operaciones, numCols14, numCols04, percColsPen4,list_Col_Date4,list_Col_pesos4, 'Operaciones Realizadas',0)


numCols05 = []
numCols15=[]
list_Col_Date5 = []
percColsPen5 = ['% Día']
list_Col_pesos5=[]
bonos_estilado = _estilador(bonos, numCols15, numCols05, percColsPen5,list_Col_Date5,list_Col_pesos5, 'Precio Bonos Globales',0)

ubicacion = str(pathlib.Path(__file__).parent)+"\\"
nombreInvActual = "Inversiones_IBM.png"

ubicacion2 = str(pathlib.Path(__file__).parent)+"\\"
nombreInvObjetivo = "Inversiones_IBM_Objetivos.png"

ubicacion3 = str(pathlib.Path(__file__).parent)+"\\"
nombreInvCapital = "Inversiones_IBM_Capital.png"

ubicacion3 = str(pathlib.Path(__file__).parent)+"\\"
nombreInvOperaciones = "Inversiones_IBM_Operaciones.png"

ubicacion5 = str(pathlib.Path(__file__).parent)+"\\"
nombrebonos = "Inversiones_IBM_Bonos.png"

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


df_to_image(cartera_estilado, ubicacion, nombreInvActual)
df_to_image(cartera2_estilado, ubicacion, nombreInvObjetivo)
df_to_image(capital_estilado, ubicacion, nombreInvCapital)
df_to_image(operaciones_estilado, ubicacion, nombreInvOperaciones)
df_to_image(bonos_estilado, ubicacion, nombrebonos)

print('Imagen Lista')

