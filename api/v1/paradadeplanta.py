from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from models.LineaBase import LineaBase
from database import db
import pandas as pd
from datetime import datetime, timedelta
import calendar
import numpy as np
import redis
import pickle
import io
import time
import os
from dotenv import load_dotenv
import math
import json

load_dotenv()
router = APIRouter()

#Funciones
        
async def id_to_string_process(cursor, array):
    
    async for item in cursor:
        item['_id'] = str(item['_id']) 
        array.append(item)
    return array

# def function_return_Streaming(df, result_list):
#     def generate():
#         buffer = io.StringIO()
#         buffer.write('[')
#         first = True
        
#         chunk_size = 1000
#         for i in range(0, len(df), chunk_size):
#             chunk = df.iloc[i:i + chunk_size]
#             if not first:
#                 buffer.write(',')
#             first = False
#             chunk_json = chunk.to_json(orient='records')
#             buffer.write(chunk_json[1:-1])
#             yield buffer.getvalue()
#             buffer.truncate(0)
#             buffer.seek(0)
#         buffer.write(']')
#         yield buffer.getvalue()
    
#     result_list.append(generate())

def function_return_Streaming(dataframes, valores_totales, result_list):
    def generate():
        buffer = io.StringIO()
        buffer.write('{')  # JSON comienza con una llave
        buffer.write(f'"AvanceReal": {valores_totales["AvanceReal"]},')
        buffer.write(f'"AvanceRealAjustado": {valores_totales["AvanceRealAjustado"]},')
        first_df = True

        for name, df in dataframes.items():
            if not first_df:
                buffer.write(',')
            first_df = False

            # Procesar cada DataFrame
            buffer.write(f'"{name}":')
            buffer.write('[')
            first_chunk = True

            chunk_size = 1000
            for i in range(0, len(df), chunk_size):
                chunk = df.iloc[i:i + chunk_size]
                if not first_chunk:
                    buffer.write(',')
                first_chunk = False

                chunk_json = chunk.to_json(orient='records')
                buffer.write(chunk_json[1:-1])  # Escribe sin los corchetes externos
                yield buffer.getvalue()
                buffer.truncate(0)
                buffer.seek(0)
            buffer.write(']')

        buffer.write('}')
        yield buffer.getvalue()  # Envía el cierre del JSON
    
    result_list.append(generate())
    


    

REDISHOST = os.getenv("REDISHOST")
REDISPORT = os.getenv("REDISPORT")
REDISUSER = os.getenv("REDISUSER")
REDISPASSWORD = os.getenv("REDISPASSWORD")


#Conectandose al servidor de redis, que entiendo esta en mi conteder de dockers
RedisDockers = redis.Redis(host=REDISHOST, port=REDISPORT,username=REDISUSER,password=REDISPASSWORD, db=0)


async def Process_LineaBase ():
    All_Data_LineaBase = []
    print("Obteniendo datos de MongoDB LineaBase")
    CursorIW37nReporte = db.Activities.find({})
    
    print("Procesando los datos de MongoDB LIneaBase")
    await id_to_string_process(CursorIW37nReporte,All_Data_LineaBase)
    df_LineaBase = pd.DataFrame(All_Data_LineaBase)
    df_Real = pd.DataFrame(All_Data_LineaBase)
        
    print("Creando el data frame de Línea base")
    df_LineaBase.sort_values(by=['id'], inplace=True)
    df_LineaBase['inicioplan'] = df_LineaBase['inicioplan'].apply(lambda x: x.replace(microsecond=100000))
    df_LineaBase['finplan'] = df_LineaBase['finplan'].apply(lambda x: x.replace(microsecond=100000))
    df_LineaBase["DifHorasTime"] = (df_LineaBase["finplan"] - df_LineaBase["inicioplan"])
    df_LineaBase["DifHorasHoras"] = ((df_LineaBase["finplan"] - df_LineaBase["inicioplan"]).dt.total_seconds() / 3600).apply(math.ceil)
    df_LineaBase['Ejex'] = df_LineaBase.apply(lambda row: [row['inicioplan'] + timedelta(hours=i) for i in range(row['DifHorasHoras'] )], axis=1)
    df_LineaBase = df_LineaBase.explode('Ejex')
    #df_LineaBase['inicioplan'] = df_LineaBase['inicioplan'].apply(lambda x: pd.to_datetime(x, unit='ms') if isinstance(x, (int, float)) else pd.to_datetime(x)).dt.strftime('%Y-%m-%dT%H:%M:%S') #solo usar para excel o power bi
    #df_LineaBase['finplan'] = df_LineaBase['finplan'].apply(lambda x: pd.to_datetime(x, unit='ms') if isinstance(x, (int, float)) else pd.to_datetime(x)).dt.strftime('%Y-%m-%dT%H:%M:%S') #solo usar para excel o power bi
    df_LineaBase['Ejex'] = df_LineaBase['Ejex'].dt.ceil('h')
    
    df_CurvaBaseAjustada = df_LineaBase[df_LineaBase['ActividadCancelada']=="No"]
    
    #df_LineaBase['Ejex'] = df_LineaBase['Ejex'].apply(lambda x: pd.to_datetime(x, unit='ms') if isinstance(x, (int, float)) else pd.to_datetime(x)).dt.strftime('%Y-%m-%dT%H:%M:%S') #solo usar para excel o power bi
    df_LineaBase_0 = df_LineaBase.groupby('Ejex')['hh'].sum().reset_index()
    df_LineaBase_0["hh cum"] = df_LineaBase_0["hh"].cumsum()
    
    print("Creando el dataframe de curva base ajustada")
    df_CurvaBaseAjustada = df_CurvaBaseAjustada.groupby('Ejex')['hh'].sum().reset_index()
    df_CurvaBaseAjustada["hh_lb_cum"] = df_CurvaBaseAjustada["hh"].cumsum()
    df_CurvaBaseAjustada.rename(columns={'hh':'hh_lb'}, inplace=True)
    
  
    
    
    print("Creando el data frame de Avance Real")
    df_Real = df_Real[df_Real['inicioreal'].notnull()]
    df_Real["TimeReference"] = pd.to_datetime('now')

    df_Real['TimeReference'] = df_Real['TimeReference'].dt.ceil('h')
    df_Real['inicioreal'] = df_Real['inicioreal'].dt.ceil('h')
    df_Real["finreal"] = df_Real["finreal"].fillna(df_Real["TimeReference"]).dt.ceil('h')
    df_Real["DifHorasTime"] = (df_Real["finreal"] - df_Real["inicioreal"])
    df_Real["DifHorasHoras"] = ((df_Real["finreal"] - df_Real["inicioreal"]).dt.total_seconds() / 3600).apply(math.ceil)
    df_Real['Ejex'] = df_Real.apply(lambda row: [row['inicioreal'] + timedelta(hours=i) for i in range(row['DifHorasHoras'] )], axis=1)
    df_Real = df_Real.explode('Ejex')
    
    df_Real['Ejex'] = df_Real['Ejex'].dt.ceil('h')
    
    df_Real_0 = df_Real.groupby('Ejex')['hh'].sum().reset_index()
    df_Real_0["hh cum"] = df_Real_0["hh"].cumsum()
    
    
    #Aca determino la fecha mas temprana y mas tardia entre la linea base y la linea real
    start_date = min(df_LineaBase_0["Ejex"].min(), df_Real_0["Ejex"].min())
    end_date = max(df_LineaBase_0["Ejex"].max(), df_Real_0["Ejex"].max())
    end_date = pd.to_datetime('2024-12-13')  #|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
    
    #Aca determino la fecha mas temprana y mas tardia entre la linea base ajustada y la linea real
    start_date2 = min(df_CurvaBaseAjustada["Ejex"].min(), df_Real_0["Ejex"].min())
    end_date2 = max(df_CurvaBaseAjustada["Ejex"].max(), df_Real_0["Ejex"].max())
    end_date2 = pd.to_datetime('2024-12-13')  #|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
    
    #Aca creo un rango de fechas con la fecha mas temprana y mas tardia entre la linea base y la linea real con saltos de una hora
    ejeXnew = pd.date_range(start=start_date, end=end_date, freq="1h")
    df_ejeXnew  = pd.DataFrame({"Ejex": ejeXnew})
    
    #Aca creo un rango de fechas con la fecha mas temprana y mas tardia entre la linea base y la linea real con saltos de una hora
    ejeXnew2 = pd.date_range(start=start_date2, end=end_date2, freq="1h")
    df_ejeXnew2  = pd.DataFrame({"Ejex": ejeXnew2})
    
    #Uniendos los dataframes de linea base y linea real y renombrando las columnas
    df_Combinado = df_ejeXnew.merge(df_LineaBase_0, on="Ejex", how="left").merge(df_Real_0, on="Ejex", how="left")
    df_Combinado.rename(columns={'hh_x': 'hh_lb', 'hh cum_x': 'hh_lb_cum', 'hh_y': 'hh_real', 'hh cum_y': 'hh_real_cum'}, inplace=True)
    df_Combinado.fillna({"hh_lb": 0, "hh_real": 0, "hh_lb_cum": 0, "hh_real_cum": 0}, inplace=True)
    
    #Uniendos los dataframes de linea base ajustada y linea real y renombrando las columnas
    df_CombinadoAjustada = df_ejeXnew2.merge(df_CurvaBaseAjustada, on="Ejex", how="left").merge(df_Real_0, on="Ejex", how="left")
    df_CombinadoAjustada.rename(columns={'hh': 'hh_real', 'hh cum': 'hh_real_cum'}, inplace=True)
    df_CombinadoAjustada.fillna({"hh_lb": 0, "hh_real": 0, "hh_lb_cum": 0, "hh_real_cum": 0}, inplace=True)
    
    #Creando las Curvas por Áreas, contratistas, TAG y bloques'
    #rename
    df_LineaBase.rename(columns={"hh":"hh_lb"}, inplace=True)
    df_LineaBase_Area = df_LineaBase.groupby(["Ejex","area"])["hh_lb"].sum().reset_index()
    df_LineaBase_Area["hh_lb_cum"] = df_LineaBase_Area["hh_lb"].cumsum()
    print(df_LineaBase_Area.columns)
    df_LineaBase_Area.rename(columns={"area":"Filtro01"}, inplace=True)
    
        
    df_LineaBase_Contratista = df_LineaBase.groupby(["Ejex","contratista"])["hh_lb"].sum().reset_index()
    df_LineaBase_Contratista["hh_lb_cum"] = df_LineaBase_Contratista["hh_lb"].cumsum()
    df_LineaBase_Contratista.rename(columns={"contratista":"Filtro01"}, inplace=True)
    
    
    df_LineaBase_TAG = df_LineaBase.groupby(["Ejex","TAG"])["hh_lb"].sum().reset_index()
    df_LineaBase_TAG["hh_lb_cum"] = df_LineaBase_TAG["hh_lb"].cumsum()
    df_LineaBase_TAG.rename(columns={"TAG":"Filtro01"}, inplace=True)
    
    
    df_LineaBase_Bloque = df_LineaBase.groupby(["Ejex","BloqueRC"])["hh_lb"].sum().reset_index() 
    df_LineaBase_Bloque["hh_lb_cum"] = df_LineaBase_Bloque["hh_lb"].cumsum()
    df_LineaBase_Bloque.rename(columns={"BloqueRC":"Filtro01"}, inplace=True)
    
    
    

    
    if df_LineaBase_0["hh cum"].max() == 0:
        AvanceReal = 0
    else:
        AvanceReal = df_Real_0["hh cum"].max()/df_LineaBase_0["hh cum"].max() 
        
    if df_CurvaBaseAjustada["hh_lb_cum"].max() == 0:
        AvanceRealAjustado = 0
    else:
        AvanceRealAjustado = df_Real_0["hh cum"].max()/df_CurvaBaseAjustada["hh_lb_cum"].max() 
    
    
    return {
        # "df_LineaBase": df_LineaBase,
        # "df_Real": df_Real,
        "df_Combinado": df_Combinado,
        "df_CombinadoAjustada": df_CombinadoAjustada,
        "AvanceReal": AvanceReal,
        "AvanceRealAjustado": AvanceRealAjustado,
        
        "df_LineaBase_Area": df_LineaBase_Area,
        "df_LineaBase_Contratista": df_LineaBase_Contratista,
        "df_LineaBase_TAG": df_LineaBase_TAG,
        "df_LineaBase_Bloque": df_LineaBase_Bloque   
    }

# def ceil_hours(dt):
#     return dt.dt.ceil('h') if isinstance(dt, pd.Series) else dt.ceil('h')

# def calc_duracion_horas(df, col_inicio, col_fin):
#     df["DifHorasTime"] = df[col_fin] - df[col_inicio]
#     df["DifHorasHoras"] = (df["DifHorasTime"].dt.total_seconds() / 3600).apply(math.ceil)
#     return df

# def expandir_por_horas(df, col_inicio, col_fin):
#     df = calc_duracion_horas(df, col_inicio, col_fin)
#     df['Ejex'] = df.apply(lambda row: [row[col_inicio] + timedelta(hours=i) for i in range(row['DifHorasHoras'])], axis=1)
#     df = df.explode('Ejex')
#     df['Ejex'] = ceil_hours(df['Ejex'])
#     return df

# async def get_dataframe_actividades():
#     All_Data_LineaBase = []
#     print("Obteniendo datos de MongoDB LineaBase")
#     cursor = db.Activities.find({})
#     await id_to_string_process(cursor, All_Data_LineaBase)
#     return pd.DataFrame(All_Data_LineaBase)

# def preparar_linea_base(df):
#     print("Procesando Línea Base")
#     df = expandir_por_horas(df, 'inicioplan', 'finplan')
#     df = df.groupby('Ejex')['hh'].sum().reset_index()
#     df["hh_lb_cum"] = df["hh"].cumsum()
#     df.rename(columns={'hh': 'hh_lb'}, inplace=True)
#     return df

# def preparar_real(df):
#     print("Procesando Avance Real")
#     df = df[df['inicioreal'].notnull()].copy()
#     df["TimeReference"] = ceil_hours(pd.to_datetime('now'))
#     df["finreal"] = df["finreal"].fillna(df["TimeReference"])
#     df = expandir_por_horas(df, 'inicioreal', 'finreal')
#     df = df.groupby('Ejex')['hh'].sum().reset_index()
#     df["hh_real_cum"] = df["hh"].cumsum()
#     df.rename(columns={'hh': 'hh_real'}, inplace=True)
#     return df

# def combinar_curvas(df_base, df_real, start=None, end=None):
#     print("Combinando curvas")
#     start = start or min(df_base["Ejex"].min(), df_real["Ejex"].min())
#     end = end or max(df_base["Ejex"].max(), df_real["Ejex"].max())
#     ejeXnew = pd.DataFrame({"Ejex": pd.date_range(start=start, end=end, freq="1h")})
    
#     combinado = ejeXnew.merge(df_base, on="Ejex", how="left").merge(df_real, on="Ejex", how="left")
#     combinado.fillna({"hh_lb": 0, "hh_real": 0, "hh_lb_cum": 0, "hh_real_cum": 0}, inplace=True)
#     return combinado

# async def Process_LineaBase():
#     df_raw = await get_dataframe_actividades()

#     df_linea_base = preparar_linea_base(df_raw.copy())
#     df_real = preparar_real(df_raw.copy())
    
#     # Ajustada: aplicar solo sobre actividades no canceladas
#     df_ajustada = preparar_linea_base(df_raw[df_raw['ActividadCancelada'] == "No"].copy())

#     # Fechas de referencia
#     start_all = min(df_linea_base['Ejex'].min(), df_real['Ejex'].min())
#     end_all = pd.to_datetime('2024-12-13')
    
#     # Combinar resultados
#     df_combinado = combinar_curvas(df_linea_base, df_real, start_all, end_all)
#     df_combinado_ajustada = combinar_curvas(df_ajustada, df_real, start_all, end_all)

#     return {
#         "df_Combinado": df_combinado,
#         "df_CombinadoAjustada": df_combinado_ajustada,
#     }


@router.get("/ProcesarLineaBase", tags=["Parada de Planta"])
async def Get_Process_BaseLine ():
    df_result = []  
    df_processed = await Process_LineaBase()
    
    valores_totales = {
        "AvanceReal": df_processed["AvanceReal"],
        "AvanceRealAjustado": df_processed["AvanceRealAjustado"]
    }

    dataframes = {
        "df_Combinado": df_processed["df_Combinado"],
        "df_CombinadoAjustada": df_processed["df_CombinadoAjustada"],
        "df_LineaBase_Area": df_processed["df_LineaBase_Area"],
        "df_LineaBase_Contratista": df_processed["df_LineaBase_Contratista"],
        "df_LineaBase_TAG": df_processed["df_LineaBase_TAG"],
        "df_LineaBase_Bloque": df_processed["df_LineaBase_Bloque"]
    }
    
    function_return_Streaming(dataframes,valores_totales,df_result)
    print("Enviando datos al frontend")
    return StreamingResponse(df_result[0], media_type='application/json')

