from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from models.ActualPlanta import ActualPlanta
from models.Provisiones import Provisiones
from database import db
from typing import List
import pandas as pd
from datetime import datetime, date
import calendar
import numpy as np
import redis
import pickle
import io
import time
import os
from dotenv import load_dotenv

load_dotenv()
router = APIRouter()

#Funciones
        
async def id_to_string_process(cursor, array):
    
    async for item in cursor:
        item['_id'] = str(item['_id']) 
        array.append(item)
    return array

def function_return_Streaming(df, result_list):
    def generate():
        buffer = io.StringIO()
        buffer.write('[')
        first = True
        
        chunk_size = 1000
        for i in range(0, len(df), chunk_size):
            chunk = df.iloc[i:i + chunk_size]
            if not first:
                buffer.write(',')
            first = False
            chunk_json = chunk.to_json(orient='records')
            buffer.write(chunk_json[1:-1])
            yield buffer.getvalue()
            buffer.truncate(0)
            buffer.seek(0)
        buffer.write(']')
        yield buffer.getvalue()
    
    result_list.append(generate())


REDISHOST = os.getenv("REDISHOST")
REDISPORT = os.getenv("REDISPORT")
REDISUSER = os.getenv("REDISUSER")
REDISPASSWORD = os.getenv("REDISPASSWORD")


#Conectandose al servidor de redis, que entiendo esta en mi conteder de dockers
RedisDockers = redis.Redis(host=REDISHOST, port=REDISPORT,username=REDISUSER,password=REDISPASSWORD, db=0)


#Proceso de Carga a Redis
@router.post("/UpdateDataSAPToRedis", tags=["Indicadores"])
async def Update_Data_SAPIndicadores_To_Redis():
    
    All_Data_IW29 = []
    All_Data_IW37nBase = []
    All_Data_IW37nReporte = []  
    All_Data_IW39 = []  
    
    Process_Status_Data_SAP_Indicadores = RedisDockers.get('Process_Status_Data_SAP_Indicadores')
    print(Process_Status_Data_SAP_Indicadores)
    
    if Process_Status_Data_SAP_Indicadores is None or Process_Status_Data_SAP_Indicadores.decode('utf-8') != 'in progess':
        
        RedisDockers.set('Process_Status_Data_SAP_Indicadores','in progess')
        print("Iniciando Carga de datos de SAP a Redis")
        
        print("Obteniendo los datos de MongoDB")
        CursorIW29 = db.iw29.find()
        cursorIW37nBase = db.iw37n.find()
        CursorIW37nReporte = db.iw37nreport.find()
        CursorIW39 = db.iw39report.find()
        
        print("Procesando los datos de MongoDB")
        await id_to_string_process(CursorIW29,All_Data_IW29)
        await id_to_string_process(cursorIW37nBase,All_Data_IW37nBase)
        await id_to_string_process(CursorIW37nReporte,All_Data_IW37nReporte)
        await id_to_string_process(CursorIW39,All_Data_IW39)
        
        print("Creando los data frame")
        df_IW29 = pd.DataFrame(All_Data_IW29)
        df_IW37nBase = pd.DataFrame(All_Data_IW37nBase)
        df_IW37nReporte = pd.DataFrame(All_Data_IW37nReporte)
        df_IW39 = pd.DataFrame(All_Data_IW39)        
        
        print("Creando las tablas en Redis")
        RedisDockers.set('df_IW29',pickle.dumps(df_IW29))
        RedisDockers.set('df_IW37nBase',pickle.dumps(df_IW37nBase))
        RedisDockers.set('df_IW37nReporte',pickle.dumps(df_IW37nReporte))
        RedisDockers.set('df_IW39',pickle.dumps(df_IW39))
        RedisDockers.set('Process_Status_Data_SAP_Indicadores','completed')
        print("Finalizando el proceso de carga de Data SAP Indicadores en Redis")
        
        return ({
            "Message": "Oki Doki"
            })


@router.get('/GetDataSAPIw39FromRedis', tags=["Indicadores"])
def Get_Data_IW39_From_Redis():
    
    print("Obteniendo datos de SAP IW39 desde redis")
    df_result = []
    pickled_IW39 =RedisDockers.get('df_IW39')
    df_IW39 = pickle.loads(pickled_IW39)
    function_return_Streaming(df_IW39,df_result)
    def generate():
        for chunk in df_result:
            yield from chunk
    print("Finalizando el proceso de obtención de IW39 desde Redis")
    return StreamingResponse(generate(), media_type='application/json')


@router.get('/GetDataSAPIw37nBaseFromRedis', tags=["Indicadores"])
def Get_Data_IW37nBase_From_Redis():
    
    print("Obteniendo datos de SAP IW37nBase desde redis")
    df_result = []
    pickled_IW37nBase =RedisDockers.get('df_IW37nBase')
    df_IW37Base = pickle.loads(pickled_IW37nBase)
    function_return_Streaming(df_IW37Base,df_result)
    def generate():
        for chunk in df_result:
            yield from chunk
    print("Finalizando el proceso de obtención de IW37nBase desde Redis")
    return StreamingResponse(generate(), media_type='application/json')


@router.get('/GetDataSAPIw37nReporteFromRedis', tags=["Indicadores"])
def Get_Data_IW37nReporte_From_Redis():
    
    print("Obteniendo datos de SAP IW37nReporte desde redis")
    df_result = []
    pickled_IW37nReporte =RedisDockers.get('df_IW37nReporte')
    df_IW37Reporte = pickle.loads(pickled_IW37nReporte)
    function_return_Streaming(df_IW37Reporte,df_result)
    def generate():
        for chunk in df_result:
            yield from chunk
    print("Finalizando el proceso de obtención de IW37nReporte desde Redis")
    return StreamingResponse(generate(), media_type='application/json')


@router.get('/GetDataSAPIw29FromRedis', tags=["Indicadores"])
def Get_Data_IW29_From_Redis():
    
    print("Obteniendo datos de SAP IW29 desde redis")
    df_result = []
    pickled_IW29 =RedisDockers.get('df_IW29')
    df_IW29 = pickle.loads(pickled_IW29)
    function_return_Streaming(df_IW29,df_result)
    def generate():
        for chunk in df_result:
            yield from chunk
    print("Finalizando el proceso de obtención de IW29 desde Redis")
    return StreamingResponse(generate(), media_type='application/json')

