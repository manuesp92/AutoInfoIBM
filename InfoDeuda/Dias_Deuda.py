
import os
import dataframe_image as dfi
from DatosLogin import login
import pandas as pd
import pyodbc #Library to connect to Microsoft SQL Server
from DatosLogin import login #Holds connection data to the SQL Server
import sys
import pathlib
from datetime import datetime
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
    conectorMSSQL = pyodbc.connect(
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


     #######################################
################ VENTAS POR COMERCIAL #####
########################################
###########################################################################
##############################   CHEQUES Clientes ###########################
###########################################################################

cheques = pd.read_sql(
 ''' 
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


select NROCLIENTE,
	SUM(r.IMPORTE) CHEQUES
	from CCRec02 as r 
		left join RecVenta as v
		on r.PTOVTAREC = v.PTOVTA
		and r.NRORECIBO = v.NRORECIBO
		and r.UEN = v.UEN
		where  FECHAVTOSQL > @hoy AND FECHASQL <= @hoy  AND FECHASQL >= '2024-01-01'
		and NROCLIENTE > 100000 and NROCLIENTE < 500000 
		and r.MEDIOPAGO in (4,10) GROUP BY NROCLIENTE
 ''' 
  ,conectorMSSQL)
cheques = cheques.convert_dtypes()
cheques['NROCLIENTE'] = cheques['NROCLIENTE'].astype(int)


deuda = pd.read_sql('''
 SELECT
            CAST(FRD.[NROCLIENTE] as VARCHAR) as 'NROCLIENTE'
            ,RTRIM(Cli.NOMBRE) as 'NOMBRE'

           ,CAST(ROUND(MIN(Cli.SALDOPREPAGO - Cli.SALDOREMIPENDFACTU),0) as int) as 'SALDOCUENTA'

            , ISNULL(RTRIM(Vend.NOMBREVEND),'') as 'Vendedor'
			, cli.LIMITECREDITO as 'Acuerdo de Descubierto $'
			, sum(FRD.importe) as 'venta en cta/cte'
            FROM [Rumaos].[dbo].[FacRemDet] as FRD with (NOLOCK)
            INNER JOIN dbo.FacCli as Cli with (NOLOCK)
                ON FRD.NROCLIENTE = Cli.NROCLIPRO
            LEFT OUTER JOIN dbo.Vendedores as Vend with (NOLOCK)
	            ON Cli.NROVEND = Vend.NROVEND

            where FRD.NROCLIENTE > 100000 and FRD.NROCLIENTE < 500000
                and Cli.ListaSaldoCC = 1
                and FECHASQL >= '20140101' and FECHASQL <= CAST(GETDATE() as date)
            group by FRD.NROCLIENTE, Cli.NOMBRE, Vend.NOMBREVEND,cli.LIMITECREDITO
            order by Cli.NOMBRE
                               
    ''' ,conectorMSSQL)
deuda = deuda.convert_dtypes()
deuda['NROCLIENTE'] = deuda['NROCLIENTE'].astype(int)
deuda = deuda.merge(cheques,on=['NROCLIENTE'],how='left')
deuda['CHEQUES'] = deuda['CHEQUES'].fillna(0)
deuda['deudaCheques'] = deuda['SALDOCUENTA'] - deuda['CHEQUES']
deuda = deuda[deuda['deudaCheques'] < -1000]
deuda['deudaCheques']=deuda['deudaCheques']*-1
# Obtener una lista de números de clientes sin corchetes
lista_clientes = deuda['NROCLIENTE'].tolist()

# Convertir la lista en una cadena separada por comas
clientes_deudores = ','.join(str(clientes) for clientes in lista_clientes)

##### Cantidades ######

cantidad = pd.read_sql(f'''

            select SUM(r.cantidad) as Cantidad,FECHASQL,r.NROCLIENTE
		 from FacRemDet as r
    left JOIN dbo.FacCli as Cli with (NOLOCK)
        ON r.NROCLIENTE = Cli.NROCLIPRO
    LEFT JOIN dbo.Vendedores as Vend with (NOLOCK)
    ON Cli.NROVEND = Vend.NROVEND
	left join facproduen as f on 
	r.CODPRODUCTO = f.CODPRODUCTO
	and r.UEN = f.UEN
    where r.FECHASQL >= '2021-01-01'
    and r.FECHASQL <= DATEADD(day, 0, CAST(GETDATE() AS date))
    and r.NROCLIENTE IN ({clientes_deudores})
	and f.CODPRODPLAYA in ('GO','EU','NS','NU')
	GROUP BY FECHASQL,NROCLIENTE
	ORDER BY FECHASQL DESC
    
 ''' ,conectorMSSQL)
cantidad = cantidad.convert_dtypes()
cantidad['NROCLIENTE'] = cantidad['NROCLIENTE'].astype(int)


# Convertir las columnas de fecha a tipo datetime
cantidad['FECHASQL'] = pd.to_datetime(cantidad['FECHASQL'])

#####  Ventas #######
ventas = pd.read_sql(f'''

         select sum(r.IMPORTE) as IMPORTE,vend.NOMBREVEND,FECHASQL,CLI.NOMBRE,r.NROCLIENTE
		 from FacRemDet as r
    left JOIN dbo.FacCli as Cli with (NOLOCK)
        ON r.NROCLIENTE = Cli.NROCLIPRO
    LEFT JOIN dbo.Vendedores as Vend with (NOLOCK)
    ON Cli.NROVEND = Vend.NROVEND
	left join facproduen as f on 
	r.CODPRODUCTO = f.CODPRODUCTO
	and r.UEN = f.UEN
    where r.FECHASQL >= '2023-01-01'
    and r.FECHASQL <= DATEADD(day, 0, CAST(GETDATE() AS date))
    and r.NROCLIENTE IN ({clientes_deudores})
	GROUP BY FECHASQL,NROCLIENTE,NOMBREVEND,NOMBRE
	ORDER BY FECHASQL DESC
    
 ''' ,conectorMSSQL)
ventas = ventas.convert_dtypes()
ventas['NROCLIENTE'] = ventas['NROCLIENTE'].astype(int)
ventas['IMPORTE'] = ventas['IMPORTE'].astype(int)
deuda['NROCLIENTE'] = deuda['NROCLIENTE'].astype(int)


# Convertir las columnas de fecha a tipo datetime
ventas['FECHASQL'] = pd.to_datetime(ventas['FECHASQL'])

# join de ventas y cantidad
ventas = ventas.merge(cantidad,on=['NROCLIENTE','FECHASQL'],how='outer')

ventas['Cantidad'] = ventas['Cantidad'].fillna(0)
# Crear un diccionario para almacenar las fechas de inicio de deuda de cada cliente
fechas_inicio_deuda = {}

# Crear una lista para almacenar los resultados como diccionarios
resultados = []

# Iterar sobre cada cliente en la tabla deudas
for i in deuda.index:
    cliente = deuda.loc[i,'NROCLIENTE']
    deuda_actual = deuda.loc[i,'deudaCheques']
    
    # Filtrar las ventas del cliente en cuestión
    ventas_cliente = ventas.loc[ventas['NROCLIENTE'] == cliente,:]
    # Ordenar las ventas por fecha en orden descendente (desde hoy hacia atrás)
    ventas_cliente = ventas_cliente.sort_values(by='FECHASQL', ascending=False)
    
    # Calcular la fecha de inicio de la deuda
    suma_ventas = 0
    suma_cantidad = 0
    fecha_inicio = None
    ventas_cliente=ventas_cliente.reset_index()
    for e in ventas_cliente.index:
        if e == 0:
            if suma_ventas <= deuda_actual:
                suma_ventas += ventas_cliente.loc[e,'IMPORTE']
                suma_cantidad += ventas_cliente.loc[e,'Cantidad']
                fecha_inicio = ventas_cliente.loc[e,'FECHASQL']
                fechas_inicio_deuda[cliente] = fecha_inicio
                cantidad = suma_cantidad
                diferencia = suma_ventas - deuda_actual
                porcentaje_pagado = (diferencia)/ventas_cliente.loc[e,'IMPORTE']
                cantidad_pagada = porcentaje_pagado*ventas_cliente.loc[e,'Cantidad']
                cantidad_adeudada = cantidad - cantidad_pagada
        else:
            if suma_ventas <= deuda_actual:
                suma_ventas += ventas_cliente.loc[e,'IMPORTE']
                suma_cantidad += ventas_cliente.loc[e,'Cantidad']
                fecha_inicio = ventas_cliente.loc[e-1,'FECHASQL']
                fechas_inicio_deuda[cliente] = fecha_inicio
                cantidad = suma_cantidad
                diferencia = suma_ventas - deuda_actual
                porcentaje_pagado = (diferencia)/ventas_cliente.loc[e,'IMPORTE']
                cantidad_pagada = porcentaje_pagado*ventas_cliente.loc[e,'Cantidad']
                cantidad_adeudada = cantidad - cantidad_pagada
            
    # Calcular la diferencia en días entre la fecha de inicio y la fecha actual
    if fecha_inicio:
        fecha_actual = datetime.now()
        dias_diferencia = (fecha_actual - fecha_inicio).days
    else:
        dias_diferencia = 500
        
    # Almacenar la información en la lista de resultados
    resultados.append({'NROCLIENTE': cliente, 'FechaInicioDeuda': fecha_inicio, 'Dias de Venta Adeudado': dias_diferencia, 'Cantidad Adeudada': cantidad_adeudada})

    
resultados_df = pd.DataFrame(resultados)
resultados_df=resultados_df.fillna('2022-01-01')
dias_Deuda=resultados_df

ubicacion = str(pathlib.Path(__file__).parent)+"\\"
nombreN = "Deudores_Comerciales.png"
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
        dfi.export(df, ubicacion+nombre,max_rows=-1)
    else:
        dfi.export(df, ubicacion+nombre,max_rows=-1)

df_to_image(resultados_df, ubicacion, nombreN)