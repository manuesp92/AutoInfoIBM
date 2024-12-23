﻿###################################
#
#     INFORME Penetración RedMás AYER
#             
#               07/10/21
###################################

import os
import sys
import pathlib

import datetime

# Allow imports from the top folder
sys.path.insert(0,str(pathlib.Path(__file__).parent.parent))

import pandas as pd
import dataframe_image as dfi
from PIL import Image

from DatosLogin import login
from Conectores import conectorMSSQL


import logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        , level=logging.INFO
)
logger = logging.getLogger(__name__)



def penetracion_liq(conexMSSQL):

    ##########################################
    # Get yesterday "PENETRACIÓN" for "Líquidos"
    ##########################################

    df_desp_x_turno_liq = pd.read_sql(
        """
            SELECT
                Despapro.UEN,
                Despapro.TURNO,
                (SELECT
                    COUNT(D.VOLUMEN) AS 'Despachos RedMas'
                FROM Rumaos.dbo.Despapro as D
                WHERE D.uen = Despapro.uen
                    AND D.TURNO = Despapro.TURNO
                    AND	D.TARJETA like 'i%'
                    AND D.FECHASQL = DATEADD(DAY,-1,CAST(GETDATE() AS date))
                    AND D.VOLUMEN > '0'
                    AND D.CODPRODUCTO <> 'GNC'
                GROUP BY D.UEN ,D.TURNO
                ) AS 'Despachos RedMas',
                COUNT(Despapro.VOLUMEN) AS 'Despachos'
            FROM Rumaos.dbo.Despapro
            WHERE Despapro.TARJETA NOT LIKE 'cc%'
                AND Despapro.FECHASQL = DATEADD(DAY,-1,CAST(GETDATE() AS date))
                AND Despapro.VOLUMEN > '0'
                AND Despapro.CODPRODUCTO <> 'GNC'
            GROUP BY Despapro.UEN,Despapro.TURNO
        """
        ,conexMSSQL)
    df_desp_x_turno_liq = df_desp_x_turno_liq.convert_dtypes()
    df_desp_x_turno_liq["UEN"] = df_desp_x_turno_liq["UEN"].str.strip()
    df_desp_x_turno_liq["TURNO"] = df_desp_x_turno_liq["TURNO"].str.strip()

    # Create a Pivot Table of "Despachos RedMas" and "Despachos" grouped by "TURNO"
    pivot_desp_totales_x_turno_liq = pd.pivot_table(df_desp_x_turno_liq
        , values=["Despachos RedMas","Despachos"]
        , columns="TURNO"
        , aggfunc="sum"
    )
    # Create column "TOTAL" with the total of each row
    pivot_desp_totales_x_turno_liq = \
        pivot_desp_totales_x_turno_liq.assign(TOTAL= lambda row: row.sum(1))
    # Create a row with the values of "Despachos RedMas" divided by "Despachos"
    pivot_desp_totales_x_turno_liq = \
        pivot_desp_totales_x_turno_liq._append(
            pivot_desp_totales_x_turno_liq.loc["Despachos RedMas"] /
            pivot_desp_totales_x_turno_liq.loc["Despachos"]
            , ignore_index=True
        )
    # Get the row with the results of the last calculation 
    total_penet_x_turno_liq = pivot_desp_totales_x_turno_liq.loc[[2]]
    # Add column UEN with value = "TOTAL"
    total_penet_x_turno_liq.insert(0,"UEN",["TOTAL"])
    # Rename index as "colTOTAL"
    total_penet_x_turno_liq.rename({2:"colTOTAL"}, inplace=True)

    #Get "PENETRACIÓN" by "UEN" and "TURNO"
    df_desp_x_turno_liq["PENETRACIÓN"] = df_desp_x_turno_liq.apply(
        lambda row: (row["Despachos RedMas"])/(row["Despachos"])
        , axis= 1
    )
    pivot_desp_x_turno_liq = pd.pivot_table(df_desp_x_turno_liq
        , values="PENETRACIÓN"
        , index="UEN"
        , columns="TURNO"
        , aggfunc="sum"
        , fill_value=0
    )
    pivot_desp_x_turno_liq.reset_index(inplace=True)


    # Get "TOTAL" of "PENETRACIÓN" by "UEN"
    pivot_desp_liq_total = pd.pivot_table(df_desp_x_turno_liq
        , values=["Despachos RedMas","Despachos"]
        , index="UEN"
        , aggfunc="sum"
        , fill_value=0
    )
    pivot_desp_liq_total.reset_index(inplace=True)
    # Create the column "TOTAL" with the totals per row
    pivot_desp_liq_total["TOTAL"] = pivot_desp_liq_total.apply(
        lambda row: (row["Despachos RedMas"])/(row["Despachos"])
        , axis= 1
    )
    pivot_desp_liq_total = pivot_desp_liq_total[["UEN","TOTAL"]]

    # Merge the column "TOTAL" to the pivot_desp_x_turno_liq
    df_penetRM_liq_x_turno = pd.merge(
        pivot_desp_x_turno_liq,
        pivot_desp_liq_total,
        on="UEN",
        how="inner"
    )
    df_penetRM_liq_x_turno.sort_values(by=["TOTAL"],inplace=True)

    # Add the row with the totals per column
    df_penetRM_liq_x_turno = pd.concat(
        [df_penetRM_liq_x_turno
            , total_penet_x_turno_liq
        ]
    )

    # print("REDMAS PENETRACION LIQ")
    # print(df_penetRM_liq_x_turno)

    if df_penetRM_liq_x_turno.columns[1] == '1':
        x=1
    elif df_penetRM_liq_x_turno.columns[1] != '1':
        df_penetRM_liq_x_turno['1'] = 0
    
    df_presupuesto_liquidos =calcularPresupuestoLiquidos()

    #total=df_presupuesto_liquidos['CANT OP RM'].sum()/df_presupuesto_liquidos['Presupuesto Mensual REDMAS'].sum() 
    #df_presupuesto_liquidos['Objetivo']=df_presupuesto_liquidos['CANT OP RM']/df_presupuesto_liquidos['Presupuesto Mensual REDMAS']
    
    total=df_presupuesto_liquidos['CANT OP RM'].sum()/df_presupuesto_liquidos['Presupuesto Mensual REDMAS'].sum()

    df_presupuesto_liquidos=df_presupuesto_liquidos.drop(columns=['Presupuesto Mensual REDMAS','CANT OP RM'])
    df_presupuesto_liquidos=df_presupuesto_liquidos.reset_index()

    df_penetRM_liq_x_turno = df_penetRM_liq_x_turno.reindex(columns=['UEN','1','2','3','TOTAL'])
    df_penetRM_liq_x_turno=  df_penetRM_liq_x_turno.merge(df_presupuesto_liquidos,on="UEN",how="outer" )
    df_penetRM_liq_x_turno=df_penetRM_liq_x_turno.fillna(0)

    # Mover las filas con "TOTAL" al final
    df_penetRM_liq_x_turno = df_penetRM_liq_x_turno.sort_values(by='UEN')
    df_penetRM_liq_x_turno = pd.concat([df_penetRM_liq_x_turno[df_penetRM_liq_x_turno['UEN'] != 'TOTAL'], df_penetRM_liq_x_turno[df_penetRM_liq_x_turno['UEN'] == 'TOTAL']])


    df_penetRM_liq_x_turno= df_penetRM_liq_x_turno.rename(index={df_penetRM_liq_x_turno.index[-1]:'colTOTAL'})
    
    df_penetRM_liq_x_turno.loc['colTOTAL', 'Objetivo']=0.25 

    print(pivot_desp_liq_total)

    #df_desvio=df_penetRM_liq_x_turno['TOTAL']-D
    df_penetRM_liq_x_turno['Desvio']=(df_penetRM_liq_x_turno['TOTAL']/df_penetRM_liq_x_turno['Objetivo'])-1

    return df_penetRM_liq_x_turno


def penetracion_GNC(conexMSSQL):

    ##########################################
    # Get yesterday "PENETRACIÓN" for "GNC"
    ##########################################

    df_desp_x_turno_GNC = pd.read_sql(
        """
            SELECT
                Despapro.UEN,
                Despapro.TURNO,
                (SELECT
                    COUNT(D.VOLUMEN) AS 'Despachos RedMas'
                FROM Rumaos.dbo.Despapro as D
                WHERE D.uen = Despapro.uen
                    AND D.TURNO = Despapro.TURNO
                    AND	D.TARJETA like 'i%'
                    AND D.FECHASQL = DATEADD(DAY,-1,CAST(GETDATE() AS date))
                    AND D.VOLUMEN > '0'
                    AND D.CODPRODUCTO = 'GNC'
                GROUP BY D.UEN ,D.TURNO
                ) AS 'Despachos RedMas',
                COUNT(Despapro.VOLUMEN) AS 'Despachos'
            FROM Rumaos.dbo.Despapro
            WHERE Despapro.TARJETA NOT LIKE 'cc%'
                AND Despapro.FECHASQL = DATEADD(DAY,-1,CAST(GETDATE() AS date))
                AND Despapro.VOLUMEN > '0'
                AND Despapro.CODPRODUCTO = 'GNC'
            GROUP BY Despapro.UEN,Despapro.TURNO
        """
        ,conexMSSQL)
    df_desp_x_turno_GNC = df_desp_x_turno_GNC.convert_dtypes()
    df_desp_x_turno_GNC["UEN"] = df_desp_x_turno_GNC["UEN"].str.strip()
    df_desp_x_turno_GNC["TURNO"] = df_desp_x_turno_GNC["TURNO"].str.strip()

    # Create a Pivot Table of "Despachos RedMas" and "Despachos" grouped by "TURNO"
    pivot_desp_totales_x_turno_GNC = pd.pivot_table(df_desp_x_turno_GNC
        , values=["Despachos RedMas","Despachos"]
        , columns="TURNO"
        , aggfunc="sum"
    )
    # Create column "TOTAL" with the total of each row
    pivot_desp_totales_x_turno_GNC = \
        pivot_desp_totales_x_turno_GNC.assign(TOTAL= lambda row: row.sum(1))
    # Create a row with the values of "Despachos RedMas" divided by "Despachos"
    pivot_desp_totales_x_turno_GNC = \
        pivot_desp_totales_x_turno_GNC._append(
            pivot_desp_totales_x_turno_GNC.loc["Despachos RedMas"] /
            pivot_desp_totales_x_turno_GNC.loc["Despachos"]
            , ignore_index=True
        )
    # Get the row with the results of the last calculation 
    total_penet_x_turno_GNC = pivot_desp_totales_x_turno_GNC.loc[[2]]
    # Add column UEN with value = "TOTAL"
    total_penet_x_turno_GNC.insert(0,"UEN",["TOTAL"])
    # Rename index as "colTOTAL"
    total_penet_x_turno_GNC.rename({2:"colTOTAL"}, inplace=True)

    # Get "PENETRACIÓN" by "UEN" and "TURNO"
    df_desp_x_turno_GNC["PENETRACIÓN"] = df_desp_x_turno_GNC.apply(
        lambda row: (row["Despachos RedMas"])/(row["Despachos"])
        , axis= 1
    )
    pivot_desp_x_turno_GNC = pd.pivot_table(df_desp_x_turno_GNC
        , values="PENETRACIÓN"
        , index="UEN"
        , columns="TURNO"
        , aggfunc="sum"
        , fill_value=0
    )
    pivot_desp_x_turno_GNC.reset_index(inplace=True)


    # Get "TOTAL" of "PENETRACIÓN" by "UEN"
    pivot_desp_GNC_total = pd.pivot_table(df_desp_x_turno_GNC
        , values=["Despachos RedMas","Despachos"]
        , index="UEN"
        , aggfunc="sum"
        , fill_value=0
    )
    pivot_desp_GNC_total.reset_index(inplace=True)
    # Create the column "TOTAL" with the totals per row
    pivot_desp_GNC_total["TOTAL"] = pivot_desp_GNC_total.apply(
        lambda row: (row["Despachos RedMas"])/(row["Despachos"])
        , axis= 1
    )
    pivot_desp_GNC_total = pivot_desp_GNC_total[["UEN","TOTAL"]]

    # Merge the column "TOTAL" to the pivot_desp_x_turno_GNC
    df_penetRM_GNC_x_turno = pd.merge(
        pivot_desp_x_turno_GNC,
        pivot_desp_GNC_total,
        on="UEN",
        how="inner"
    )
    df_penetRM_GNC_x_turno.sort_values(by=["TOTAL"],inplace=True)

    # Add the row with the totals per column
    df_penetRM_GNC_x_turno = pd.concat(
        [df_penetRM_GNC_x_turno
            , total_penet_x_turno_GNC
        ]
    )

    # print("\nREDMAS PENETRACION GNC")
    # print(df_penetRM_GNC_x_turno)
    if df_penetRM_GNC_x_turno.columns[1] == '1':
        x=1
    elif df_penetRM_GNC_x_turno.columns[1] != '1':
        df_penetRM_GNC_x_turno['1'] = 0
    
    #Columna objetivo
    df_presupuesto_gnc=calcularPresupuestoGNC()
    total=df_presupuesto_gnc['CANT OP RM'].sum()/df_presupuesto_gnc['Presupuesto Mensual REDMAS'].sum() 
    df_presupuesto_gnc['Objetivo']= 0.65  # df_presupuesto_gnc['CANT OP RM']/df_presupuesto_gnc['Presupuesto Mensual REDMAS']
    df_presupuesto_gnc=df_presupuesto_gnc.drop(columns=['Presupuesto Mensual REDMAS','CANT OP RM'])
    

    df_penetRM_GNC_x_turno = df_penetRM_GNC_x_turno.reindex(columns=['UEN','1','2','3','TOTAL'])
    df_penetRM_GNC_x_turno=  df_penetRM_GNC_x_turno.merge(df_presupuesto_gnc,on="UEN",how="outer" )
    df_penetRM_GNC_x_turno=df_penetRM_GNC_x_turno.fillna(0)
    
    # Mover las filas con "TOTAL" al final
    df_penetRM_GNC_x_turno = df_penetRM_GNC_x_turno.sort_values(by='UEN')
    df_penetRM_GNC_x_turno = pd.concat([df_penetRM_GNC_x_turno[df_penetRM_GNC_x_turno['UEN'] != 'TOTAL'], df_penetRM_GNC_x_turno[df_penetRM_GNC_x_turno['UEN'] == 'TOTAL']])


    df_penetRM_GNC_x_turno= df_penetRM_GNC_x_turno.rename(index={df_penetRM_GNC_x_turno.index[-1]:'colTOTAL'})

    df_penetRM_GNC_x_turno.loc['colTOTAL', 'Objetivo']=0.65
    #df_penetRM_GNC_x_turno.loc[df_penetRM_GNC_x_turno['objetivo'].isna(),'objetivo']=total

    df_penetRM_GNC_x_turno['Desvio']=(df_penetRM_GNC_x_turno['TOTAL']/df_penetRM_GNC_x_turno['Objetivo'])-1


    return df_penetRM_GNC_x_turno


##############
# STYLING of the dataframe
##############

def _estiladorVtaTitulo(df,listaColNumericas,titulo):
    """
This function will return a styled dataframe that must be assign to a variable.
ARGS:
    df: Dataframe that will be styled.
    listaColNumericas: List of numeric columns that will be formatted 
    as percentage.
    titulo: String for the table caption.
    """

    resultado = df.style \
        .format("{:,.2%}", subset=listaColNumericas) \
        .hide(axis=0) \
        .set_caption(titulo
            +"\n"
            +((pd.to_datetime("today")-pd.to_timedelta(1,"days"))
            .strftime("%d/%m/%y"))
        ) \
        .set_properties(subset=listaColNumericas
            , **{"text-align": "center", "width": "50px"}) \
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
        .applymap(table_color,subset='Desvio')\
        .apply(lambda x: ["background: black" if x.name == "colTOTAL" 
            else "" for i in x]
            , axis=1) \
        .apply(lambda x: ["color: white" if x.name == "colTOTAL" 
            else "" for i in x]
            , axis=1)

    #Gradient color for column "TOTAL" without affecting row "TOTAL"
    evitarTotales = df.index.get_level_values(0)
    resultado = resultado.background_gradient(
        cmap="summer_r"
        ,vmin=0
        ,vmax=1
        ,subset=pd.IndexSlice[evitarTotales[:-1],"TOTAL"]
    )
    
    return resultado

def table_color(val):
    """
    Takes a scalar and returns a string with
    the css property `'color: red'` for less than 60 marks, green otherwise.
    """
    color = 'green' if val > 0 else 'red'
    return 'color: % s' % color

##############
# PRINTING dataframe as an image
##############

# This will print the df with a unique name and will erase the old image 
# everytime the script is run

def _df_to_image(df, ubicacion, nombre):
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


##############
# MERGING images
##############

def _append_images(listOfImages, direction='horizontal',
                  bg_color=(255,255,255), alignment='center'):
    """
    Appends images in horizontal/vertical direction.

    Args:
        listOfImages: List of images with complete path
        direction: direction of concatenation, 'horizontal' or 'vertical'
        bg_color: Background color (default: white)
        alignment: alignment mode if images need padding;
           'left', 'right', 'top', 'bottom', or 'center'

    Returns:
        Concatenated image as a new PIL image object.
    """
    images = [Image.open(x) for x in listOfImages]
    widths, heights = zip(*(i.size for i in images))

    if direction=='horizontal':
        new_width = sum(widths)
        new_height = max(heights)
    else:
        new_width = max(widths)
        new_height = sum(heights)

    new_im = Image.new('RGB', (new_width, new_height), color=bg_color)

    offset = 0
    for im in images:
        if direction=='horizontal':
            y = 0
            if alignment == 'center':
                y = int((new_height - im.size[1])/2)
            elif alignment == 'bottom':
                y = new_height - im.size[1]
            new_im.paste(im, (offset, y))
            offset += im.size[0]
        else:
            x = 0
            if alignment == 'center':
                x = int((new_width - im.size[0])/2)
            elif alignment == 'right':
                x = new_width - im.size[0]
            new_im.paste(im, (x, offset))
            offset += im.size[1]

    return new_im

def calcularPresupuestoGNC():
    fecha_actual = datetime.date.today()
    fecha_actual= fecha_actual - datetime.timedelta(days=1)
    fecha_actual=fecha_actual.replace(day=1).strftime("%Y/%m/%d")

    ruta_presupuestos='C:/Informes/Presupuestos/PPTO KPI.xlsx'
    df_presupuesto= pd.read_excel(ruta_presupuestos, sheet_name='red mas')
        #df_presupuesto[['despacho'],['despacho red mas']]        
    df_presupuesto= df_presupuesto.convert_dtypes().fillna(0)

    df_presupuesto['UEN']=df_presupuesto['UEN'].str.strip()
        
    df_presupuesto=df_presupuesto.loc[df_presupuesto["fecha"]==fecha_actual]
    df_presupuesto_gnc=df_presupuesto.loc[df_presupuesto['COMBUSTIBLE']=='GNC']
    df_presupuesto_gnc=df_presupuesto_gnc.reindex(columns=['UEN', 'Presupuesto Mensual REDMAS', 'CANT OP RM'])

    #df_presupuesto_gnc=pd.concat([df_presupuesto_gnc, pd.DataFrame(total).transpose()],ignore_index=True)
    return df_presupuesto_gnc    

def calcularPresupuestoLiquidos():
    fecha_actual = datetime.date.today()
    fecha_actual= fecha_actual - datetime.timedelta(days=1)
    fecha_actual=fecha_actual.replace(day=1).strftime("%Y/%m/%d")

    ruta_presupuestos='C:/Informes/Presupuestos/PPTO KPI.xlsx'
    df_presupuesto= pd.read_excel(ruta_presupuestos, sheet_name='red mas')
        #df_presupuesto[['despacho'],['despacho red mas']]
    df_presupuesto['UEN']=df_presupuesto['UEN'].str.strip()

    df_presupuesto= df_presupuesto.convert_dtypes().fillna(0)

    df_presupuesto=df_presupuesto.loc[df_presupuesto["fecha"]==fecha_actual]

    df_presupuesto_liquidos=df_presupuesto.loc[df_presupuesto['COMBUSTIBLE']!='GNC']

    df_presupuesto_liquidos=df_presupuesto_liquidos.reindex(columns=['UEN', 'Presupuesto Mensual REDMAS', 'CANT OP RM'])

    df_presupuesto_liquidos_sumados = df_presupuesto_liquidos.groupby('UEN').sum()
    
    #df_presupuesto_liquidos_sumados['Objetivo']=(df_presupuesto_liquidos_sumados['CANT OP RM']/df_presupuesto_liquidos_sumados['Presupuesto Mensual REDMAS'])
    
    df_presupuesto_liquidos_sumados['Objetivo']= 0.25
    

    return df_presupuesto_liquidos_sumados

##############
# FUNCTION TO RUN MODULE
##############

def penetracionRedMas():
    '''
    This function will create 3 .png files at the module 
    folder ("penetracionRedMas_liq", "penetracionRedMas_GNC" and
    "Info_PenetracionRedMas") and will display total time elapsed
    '''
    # Timer
    tiempoInicio = pd.to_datetime("today")

    conexMSSQL = conectorMSSQL(login)

    df_penetRM_liq_x_turno_Estilo = _estiladorVtaTitulo(
        penetracion_liq(conexMSSQL)
        , ["1","2","3","TOTAL",'Objetivo','Desvio']
        , "Penetración RedMas: Líquidos"
    )
    

    df_penetRM_GNC_x_turno_Estilo = _estiladorVtaTitulo(
        penetracion_GNC(conexMSSQL)
        , ["1","2","3","TOTAL",'Objetivo','Desvio']
        , "Penetración RedMas: GNC"
    )

    

    ubicacion = str(pathlib.Path(__file__).parent)+"\\"
    nombreIMG_liq = "penetracionRedMas_liq.png"
    nombreIMG_GNC = "penetracionRedMas_GNC.png"

    _df_to_image(df_penetRM_liq_x_turno_Estilo, ubicacion, nombreIMG_liq)
    _df_to_image(df_penetRM_GNC_x_turno_Estilo, ubicacion, nombreIMG_GNC)

    listaImagenes = [ubicacion + nombreIMG_liq, ubicacion + nombreIMG_GNC]
        
    fusionImagenes = _append_images(listaImagenes)
    fusionImagenes.save(ubicacion + "Info_PenetracionRedMas.png")

    # Timer
    tiempoFinal = pd.to_datetime("today")
    logger.info(
        "\nInfo Penetración RedMas"
        + "\nTiempo de Ejecucion Total: "
        + str(tiempoFinal-tiempoInicio)
    )



if __name__ == "__main__":
    penetracionRedMas()