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

REDISHOST = os.getenv("REDISHOST")
REDISPORT = os.getenv("REDISPORT")
REDISUSER = os.getenv("REDISUSER")
REDISPASSWORD = os.getenv("REDISPASSWORD")


#Conectandose al servidor de redis, que entiendo esta en mi conteder de dockers
RedisDockers = redis.Redis(host=REDISHOST, port=REDISPORT,username=REDISUSER,password=REDISPASSWORD, db=0)

#Proceso de Finanzas
@router.post("/UpdateDataFinanzasToRedis", tags=["Costos"])
async def Update_Data_Finanzas_To_Redis():
    
    All_Data_CeCo = []
    All_Data_ClaseCostos = []
    All_Data_Partidas = []  
    
    Process_Status_Data_Finanzas = RedisDockers.get('Process_Status_Data_Finanzas')
    print(Process_Status_Data_Finanzas)
    
    if Process_Status_Data_Finanzas is None or Process_Status_Data_Finanzas.decode('utf-8') != 'in progess':
        
        RedisDockers.set('Process_Status_Data_Finanzas','in progess')
        print("Iniciando Carga de datos de finanzas a Redis")
        
        CursorCeCo = db.cecos.find()
        cursorClaseCosto = db.clasecostos.find()
        CursorPartidas = db.partidas.find()
        
        await id_to_string_process(CursorCeCo,All_Data_CeCo)
        await id_to_string_process(cursorClaseCosto,All_Data_ClaseCostos)
        await id_to_string_process(CursorPartidas,All_Data_Partidas)
        
        df_CeCo = pd.DataFrame(All_Data_CeCo)
        df_ClaseCostos = pd.DataFrame(All_Data_ClaseCostos)
        df_Partidas = pd.DataFrame(All_Data_Partidas)
        
        #print(len(df_ClaseCostos))
        
        df_CeCo = df_CeCo.drop_duplicates(subset=['CeCo'])
        df_ClaseCostos = df_ClaseCostos.drop_duplicates(subset=['ClaseCosto'])
        df_Partidas = df_Partidas.drop_duplicates(subset=['Partida'])
        
        
        RedisDockers.set('df_CeCo',pickle.dumps(df_CeCo))
        RedisDockers.set('df_ClaseCostos',pickle.dumps(df_ClaseCostos))
        RedisDockers.set('df_Partidas',pickle.dumps(df_Partidas))
        print("Proceso de Carga de datos de finanzas a Redis")
        RedisDockers.set('Process_Status_Data_Finanzas','completed')
        
        
        
        return ({
            "Message": "Oki Doki"
            })


@router.get('/GetDataFinanzasFromRedis', tags=["Costos"])
async def Get_Data_Finanzas_From_Redis():
    
    print("Obteniendo datos de finanzas de redis")
    pickled_CeCo = RedisDockers.get('df_CeCo')
    pickled_ClaseCostos =RedisDockers.get('df_ClaseCostos')
    pickled_Partidas =RedisDockers.get('df_Partidas')
    
    df_CeCo = pickle.loads(pickled_CeCo)
    df_ClaseCostos = pickle.loads(pickled_ClaseCostos)
    df_Partidas = pickle.loads(pickled_Partidas)
    
    df_CeCo = df_CeCo.where(pd.notnull(df_CeCo), '')
    df_ClaseCostos = df_ClaseCostos.where(pd.notnull(df_ClaseCostos), '')
    df_Partidas = df_Partidas.where(pd.notnull(df_Partidas), '')
    
    data_CeCo = df_CeCo.to_dict(orient='records')
    data_ClaseCostos = df_ClaseCostos.to_dict(orient='records')
    data_Partidas = df_Partidas.to_dict(orient='records')
    
    print("Finalizando el proceso de Obteniendo datos de finanzas desde redis")
    
    return {
        "data_CeCo": data_CeCo,
        "data_ClaseCostos": data_ClaseCostos,
        "data_Partidas": data_Partidas,
    }


#Proceso de Provisiones
@router.post("/UpdateDataProvisionesToRedis", tags=["Costos"])
async def Update_Data_Provisiones_To_Redis():
    All_Data_Provisiones = []
    
    Process_Status_Data_Provisiones = RedisDockers.get('Process_Status_Data_Provisiones')
    if Process_Status_Data_Provisiones is None or Process_Status_Data_Provisiones != 'in progess':
        
        print("Iniciando proceso de carga de datos de provisiones en Redis")
        RedisDockers.set('Process_Status_Data_Provisiones','in progess')
        CursorProvisiones = db.provisiones.find()
        
        await id_to_string_process(CursorProvisiones,All_Data_Provisiones)
        df_Provisiones = pd.DataFrame(All_Data_Provisiones)
        
        print(df_Provisiones.columns)
        
        RedisDockers.set('df_Provisiones',pickle.dumps(df_Provisiones))
        print("Finalizando el proceso de carga de datos de provisiones en Redis")
        RedisDockers.set('Process_Status_Data_Provisiones','completed')
        
        
        return{
            "Message": "Oki Doki"
        }


@router.get("/GetDataProvisionesFromRedis", tags=["Costos"])
async def Get_Data_Provisiones_From_Redis():

    print("Ejecutando get data de provisiones redis")
    pickled_df = RedisDockers.get('df_Provisiones')
    df_combined = pickle.loads(pickled_df)
    # print(df_combined.columns)

    def generate():
        buffer = io.StringIO()
        yield '['
        first = True

        # Configura el tamaño del chunk
        chunk_size = 1000
        for i in range(0, len(df_combined), chunk_size):
            chunk = df_combined.iloc[i:i+chunk_size]
            if not first:
                buffer.write(',')
            first = False
            # Convierte el chunk a JSON
            buffer.write(chunk.to_json(orient='records')[1:-1])
            yield buffer.getvalue()
            buffer.truncate(0)
            buffer.seek(0)
        yield ']'

    # Retorna la respuesta en streaming
    print("Finalizado el proceso de obtencion de provisiones desde redis")
    return StreamingResponse(generate(), media_type='application/json')    


#Proceso de Budget
@router.post("/UpdateDataBudgetToRedis", tags=["Costos"])
async def Update_Data_Budget_Redis():
    
    print("Iniciando proceso de carga de actual en Redis")
    All_Data_Budget = []
    
    Process_Status_Data_Budget = RedisDockers.get('Process_Status_Data_Budget')
    if Process_Status_Data_Budget is None or Process_Status_Data_Budget != 'in progess':
        
        print("Obteniendo datos actual de redis")
        
        pickled_df = RedisDockers.get('df_combined')
        df_Actual = pickle.loads(pickled_df)
        #print(df_Actual.columns)
        
        RedisDockers.set('Process_Status_Data_Budget','in progess')
        print("Obteniendo datos budget de MongoDB")
        CursorBudget = db.budgetplanta.find()
        
        print("Procesando la información del Budget")
        await id_to_string_process(CursorBudget,All_Data_Budget)
        df_Budget_Mongo = pd.DataFrame(All_Data_Budget)
        
        columnas_budget = [
        "Gerencia", "Planta", "Area", "SubArea", "Categoria", 
        "CeCo", "DescripcionCeCo", "ClaseCosto", "DescripcionClaseCosto", "Responsable", 
        "Especialidad", "Partida", "DescripcionPartida", 
        "Mes","Monto","PptoForecast","TxtPedido","CN","TAG","Justificacion","Proveedor","OC","Posicion","Fecha",
        ]
        
        df_Budget = df_Budget_Mongo[columnas_budget]
        df_Budget['Mes'] = pd.to_datetime(df_Budget['Mes'].apply(lambda x: date(2024, x, 1)))
        df_Budget['Mes'] = pd.to_datetime(df_Budget['Mes'], unit='ms').dt.strftime('%Y-%m-%dT%H:%M:%S')
        #print(df_Budget.columns)
        
        df_combined = pd.concat([df_Actual, df_Budget], ignore_index=True)
        df_combined['CN'] = df_combined['CN'].fillna(0)
        
        RedisDockers.set('df_Budget',pickle.dumps(df_combined))
        RedisDockers.set('Process_Status_Data_Budget','completed')
        print("Finalizando proceso de carga de datos actual desde redis")
        
        return{
            "Message": "Oki Doki"
        }


@router.get("/GetDataBudgetFromRedis", tags=["Costos"])
async def Get_Data_Budget_From_Redis():

    print("Ejecutando get data de Budget from redis")
    pickled_df = RedisDockers.get('df_Budget')
    df_Budget = pickle.loads(pickled_df)
    # print(df_combined.columns)

    def generate():
        buffer = io.StringIO()
        yield '['
        first = True

        # Configura el tamaño del chunk
        chunk_size = 1000
        for i in range(0, len(df_Budget), chunk_size):
            chunk = df_Budget.iloc[i:i+chunk_size]
            if not first:
                buffer.write(',')
            first = False
            # Convierte el chunk a JSON
            buffer.write(chunk.to_json(orient='records')[1:-1])
            yield buffer.getvalue()
            buffer.truncate(0)
            buffer.seek(0)
        yield ']'

    # Retorna la respuesta en streaming
    print("Finalizado el proceso de obtencion de Budget desde redis")
    return StreamingResponse(generate(), media_type='application/json')    


#Proceso de Actual
@router.post("/UpdateDataActualToRedis/{CurrentMonth}", tags=["Costos"])
async def Update_Data_Actual_To_Redis(CurrentMonth: int):
    
    All_Data_Actual = [] 
    
    Process_Status_Data_Actual = RedisDockers.get('Process_Status_Data_Actual')
    if Process_Status_Data_Actual is None or Process_Status_Data_Actual != 'in progess':
        
        print("Iniciando Proceso de carga de datos de actual a Redis")
        RedisDockers.set('Process_Status_Data_Actual','in progress')
        
        print("Obteniendo Datos de MongoDB")
        CursorActual = db.actualplanta.find({
            'Mes': {
                '$ne':0,
                '$lt':CurrentMonth
                    }
        })
        
        await id_to_string_process(CursorActual,All_Data_Actual)
        df_Actual = pd.DataFrame(All_Data_Actual)
        df_Actual['CategoriaActual'] = df_Actual['CategoriaActual'].fillna("Real")
        
        print("Guardando datos en Redis")
        RedisDockers.set('df_Actual',pickle.dumps(df_Actual))
        RedisDockers.set('Process_Status_Data_Actual','completed')
        print("Finalizando Proceso de Carga de datos de actual a Redis")

        
        return{
            "Message": "Oki Doki"
        }    


@router.get("/PyProcessDataActual/{ProvMonth}", tags=["Costos"])
async def Get_Data_Actual_Planta(ProvMonth: int):
    
    pd.set_option('future.no_silent_downcasting', True)
    
    start_time = time.time()
    
    print("Iniciando proceso de procesamiento de datos de Actual")
    processing_status = RedisDockers.get('processing_status')
    if processing_status is None or processing_status.decode('utf-8') != 'in progess':
        print("Inicio de procesamiento de datos")
    
        #Variables
        
        print("Definiendo variables")
        all_items_Actual = []
        
        #Proceso Datos de Finanzas
        
        print("Obteniendo datos de finanzas")
        Undo_pickled_df_CeCo = RedisDockers.get('df_CeCo')
        Undo_pickled_df_ClaseCostos = RedisDockers.get('df_ClaseCostos')
        Undo_pickled_df_Partidas = RedisDockers.get('df_Partidas')
        
        dfCeCo = pickle.loads(Undo_pickled_df_CeCo)
        dfClaseCosto = pickle.loads(Undo_pickled_df_ClaseCostos)
        dfPartida = pickle.loads(Undo_pickled_df_Partidas)     
        
        #Proceso de Actual
        
        print("Consultando datos de Actual planta en MongDB")
        cursorActual = db.actualplanta.find({"Mes": {"$ne": 0, "$gte":ProvMonth}})
        
        print("Obteniendo datos actual de redis")
        Undo_Pickled_Actual = RedisDockers.get('df_Actual')
        dfActual_redis = pickle.loads(Undo_Pickled_Actual)
        
        print("Procesando datos Actual")
        
        await id_to_string_process(cursorActual,all_items_Actual)
        dfActual = pd.DataFrame(all_items_Actual)
        dfActual['CategoriaActual'] = dfActual['CategoriaActual'].fillna("Real")
        dfActual = pd.concat([dfActual, dfActual_redis], ignore_index=True)
        
        
        dfActual['Mes'] = pd.to_datetime(dfActual['Mes'].apply(lambda x: date(2024, x, 1)))
        dfActual['_id'] = dfActual['_id'].astype(str)
        
        dfActual = dfActual.where(pd.notnull(dfActual), None)
        
        #Proceso de Provisiones
        
        print("Obteniendo datos provision de redis")
        Undo_Pickled_Provision = RedisDockers.get('df_Provisiones')
        dfProvision_redis = pickle.loads(Undo_Pickled_Provision)
        
        print(f"provision 1: {dfProvision_redis.shape[0]}")
        
        dfProvision_redis = dfProvision_redis[dfProvision_redis['Status'] == 'Aprobado']
        dfProvision_redis = dfProvision_redis[dfProvision_redis['TipoProvision'] != 'ReProvisión']
        dfProvision_redis = dfProvision_redis[dfProvision_redis['CeCo'].astype(str).str.startswith('22')]
        
                
        dfProvision_redis['FechaEnvioProvision']  = pd.to_datetime(dfProvision_redis['FechaEnvioProvision'].astype(int), origin='1899-12-30', unit='D')
        dfProvision_redis['FechaEnvioProvision']  = dfProvision_redis['FechaEnvioProvision'].dt.date
        dfProvision_redis = dfProvision_redis[dfProvision_redis['FechaEnvioProvision'] > date(2024,ProvMonth,1)]
        
        columnas_provisiones = [
        "ClaseCosto", "DescClaseCosto", "CeCo", "DescCeCo", "NombreProveedor", 
        "FechaEnvioProvision", "OC", "Posicion", "Monto", "Moneda", 
        "Partida", "DescripcionServicio", "Planta", 
        "TipoProvision"
        ]
        
        #Manteniendo solo las columnas que quiero
        dfProvision = dfProvision_redis[columnas_provisiones]
        print(f"provision 2: {dfProvision.shape[0]}")   
        
        print(f"provision 3 (Condicional): {dfProvision_redis.shape[0]}")
        
        if dfProvision_redis.shape[0]>0:                 
            print("Procesando datos provisiones")  
            
            
            dfProvision = dfProvision.where(pd.notnull(dfProvision), None) 
            dfProvision.loc[dfProvision['Moneda'] == 'PEN', 'Monto'] = dfProvision['Monto'] / 3.7
            
            dfProvision.drop(columns=['Moneda'], inplace=True)
            dfProvision.drop(columns=['DescClaseCosto'], inplace=True)
            dfProvision.drop(columns=['DescCeCo'], inplace=True)
            
            dfProvision['FechaEnvioProvision'] = pd.to_datetime(dfProvision['FechaEnvioProvision'].astype(int), origin='1899-12-30', unit='D')
            dfProvision['FechaEnvioProvision'] = dfProvision['FechaEnvioProvision'].dt.to_period('M').dt.to_timestamp()
            
            dfProvision = dfProvision.merge(dfCeCo[['CeCo','DescripcionCeCo','Area','SubArea']],on='CeCo',how='left')
            dfProvision = dfProvision.merge(dfClaseCosto[['ClaseCosto','DescripcionClaseCosto']],on='ClaseCosto',how='left')
            dfProvision = dfProvision.merge(dfPartida[['Partida','DescripcionPartida']],on='Partida',how='left')
            
            dfProvision['Categoria'] = 'Servicios' 
            dfProvision['CategoriaActual'] = 'Provisión' 
            dfProvision['PptoForecast'] = 'Actual' 
            dfProvision['CN'] = '0'
            
            dfProvision.rename(columns={'FechaEnvioProvision': 'Mes'}, inplace=True)
            dfProvision.rename(columns={'DescClaseCosto': 'DescripcionClaseCosto'}, inplace=True)
            dfProvision.rename(columns={'NombreProveedor': 'Proveedor'}, inplace=True)
            dfProvision.rename(columns={'DescripcionServicio': 'TxtPedido'}, inplace=True)
            
            print(dfProvision.columns)
            print(f"provision 4: {dfProvision.shape[0]}")
                        
            
        dfProvision = dfProvision.where(pd.notnull(dfProvision), '')
    
        df_combined = pd.concat([dfActual, dfProvision], ignore_index=True)
        df_combined['Monto'] = df_combined['Monto'].fillna(0)
        df_combined = df_combined.where(pd.notnull(df_combined), '')
        df_combined['Area'] = df_combined['Area'].apply(lambda x: x.strip() if isinstance(x, str) else x)
        df_combined['SubArea'] = df_combined['SubArea'].apply(lambda x: x.strip() if isinstance(x, str) else x)
        df_combined['DescripcionCeCo'] = df_combined['DescripcionCeCo'].apply(lambda x: x.strip() if isinstance(x, str) else x)
        df_combined['DescripcionClaseCosto'] = df_combined['DescripcionClaseCosto'].apply(lambda x: x.strip() if isinstance(x, str) else x)
        
        df_combined['Mes'] = pd.to_datetime(df_combined['Mes'], unit='ms').dt.strftime('%Y-%m-%dT%H:%M:%S')

        
        nan_counts = df_combined.isna().sum()
        #print(nan_counts)
        
        
        #Almacenando en Redis
        RedisDockers.set('df_combined',pickle.dumps(df_combined))        
        
    
    #Enviando Datos
    print(f"Cantidad de filas: {df_combined.shape[0]}")
    print("enviando los datos via streaming")
    
    df_combined = pickle.loads(RedisDockers.get('df_combined'))
    
    def generate():
        buffer = io.StringIO()
        yield '['
        first = True
        start_time = time.time()
        chunk_size = 1000
        for i in range(0, len(df_combined), chunk_size):
            chunk = df_combined.iloc[i:i+chunk_size]
            if not first:
                buffer.write(',')
            first = False
            buffer.write(chunk.to_json(orient='records')[1:-1])
            yield buffer.getvalue()
            buffer.truncate(0)
            buffer.seek(0)
        yield ']'
        end_time = time.time()
        print(f"Streaming completed in {end_time - start_time} seconds")
        RedisDockers.set('processing_status', 'not_started')
        
    end_time = time.time()
    print("Finalizando proceso de procesamiento de datos de Actual")
    print(f"Streaming completed in {end_time - start_time} seconds")

    #return StreamingResponse(generate(), media_type='application/json')
    return ({
            "Message": "Oki Doki"
            })


@router.get("/GetDataActual", tags=["Costos"])
async def Get_Data_Actual():
    print("Obteniendo datos actual desde redis")

    pickled_df = RedisDockers.get('df_combined')
    df_combined = pickle.loads(pickled_df)

    def generate():
        buffer = io.StringIO()
        yield '['
        first = True

        # Configura el tamaño del chunk
        chunk_size = 1000
        for i in range(0, len(df_combined), chunk_size):
            chunk = df_combined.iloc[i:i+chunk_size]
            if not first:
                buffer.write(',')
            first = False
            # Convierte el chunk a JSON
            buffer.write(chunk.to_json(orient='records')[1:-1])
            yield buffer.getvalue()
            buffer.truncate(0)
            buffer.seek(0)
        yield ']'

    # Retorna la respuesta en streaming
    print("Finalizando el proceso de obtención de Actual desde Redis")
    return StreamingResponse(generate(), media_type='application/json')    




# @router.get("/GetItems", response_model=List[ActualPlanta])
# async def read_items():
#     cursor = db.actualplanta.find({"Mes": {"$ne": 0}})
#     all_items = []
    
    
#     async for item in cursor:
#         item['_id'] = str(item['_id'])
#         all_items.append(item)
#         #print(item)    
#     df = pd.DataFrame(all_items)
#     summary = df.groupby('Planta')['Monto'].sum().reset_index()
#     transformed_data = summary.to_dict(orient='records')
#     print("------")
#     print("Esta es la data trasnformada", transformed_data)
#     print("------")
#     return transformed_data


# @router.get("/GetAllDataProvisiones", response_model=List[Provisiones])
# async def read_items():
#     cursor = db.provisiones.find()
#     all_items = []
    
    
#     async for item in cursor:
#         item['_id'] = str(item['_id'])
#         all_items.append(item)
        
#     df = pd.DataFrame(all_items)
#     df = df.where(pd.notnull(df), None)  
#     transformed_data = df.to_dict(orient='records')
#     return transformed_data