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
import time,datetime
import pytz
from typing import Optional

load_dotenv()
router = APIRouter()

print("Prueba de commit")
#Fechas
def get_current_datetime():
    peru_tz = pytz.timezone("America/Lima")
    current_date = datetime.datetime.now(peru_tz)
    current_day = current_date.weekday()
    current_week = current_date.isocalendar()[1]
    current_month = current_date.month
    current_year = current_date.year

    last_date = current_date - datetime.timedelta(days=7)
    #last_date = current_date - datetime.timedelta(days=14) #Para correr el mensual
    last_day = last_date.weekday()
    last_week = last_date.isocalendar()[1]
    last_month = last_date.month
    last_year = last_date.year

    if current_day == 0:
        Semana = last_week
        Anho = last_year
    else:
        Semana = current_week
        Anho = current_year
    #Semana = last_week #Borrar
    #Semana = "18" #Borrar esto es para el mensual
    print("Semana: ",Semana,"Anho: ",Anho, "Anho anterior: ",Anho-1)
    return current_date, Semana, Anho
    

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

@router.get("/GetDates", tags=["Indicadores"])
async def Get_Dates():
    data = get_current_datetime()

    return {
        "Message": "Oki Doki",
    }

@router.post("/ResetStatusRedis",tags=["Indicadores"])
async def ResetStatusRedis():
    RedisDockers.set('Process_Status_Data_SAP_Indicadores','completed')
    return ({"Message": "Oki Doki"})


#Proceso de Carga a Redis
@router.post("/UpdateDataIndicadoresToRedisAnual", tags=["Indicadores"])
async def Update_Data_Indicadores_To_Redis_Anual():
    
    All_Data_IW29 = []
    All_Data_IW37nBase = []
    All_Data_IW37nReporte = []  
    All_Data_IW39 = []  
    
    Process_Status_Data_SAP_Indicadores = RedisDockers.get('Process_Status_Data_SAP_Indicadores')
    print(Process_Status_Data_SAP_Indicadores)
    
    if Process_Status_Data_SAP_Indicadores is None or Process_Status_Data_SAP_Indicadores.decode('utf-8') != 'in progess':
        
        RedisDockers.set('Process_Status_Data_SAP_Indicadores','in progess')
        
        CursorIW29 = db.iw29.find({"Anho": "2025"})
        cursorIW37nBase = db.iw37n.find({"Anho": "2025"})
        CursorIW37nReporte = db.iw37nreport.find({"Anho": "2025"})
        CursorIW39 = db.iw39report.find({"Anho": "2025"})
        
        await id_to_string_process(CursorIW29,All_Data_IW29)
        await id_to_string_process(cursorIW37nBase,All_Data_IW37nBase)
        await id_to_string_process(CursorIW37nReporte,All_Data_IW37nReporte)
        await id_to_string_process(CursorIW39,All_Data_IW39)
        
        df_IW29 = pd.DataFrame(All_Data_IW29)
        df_IW37nBase = pd.DataFrame(All_Data_IW37nBase)
        df_IW37nReporte = pd.DataFrame(All_Data_IW37nReporte)
        df_IW39 = pd.DataFrame(All_Data_IW39)        
        
        RedisDockers.set('df_IW29',pickle.dumps(df_IW29))
        RedisDockers.set('df_IW37nBase',pickle.dumps(df_IW37nBase))
        RedisDockers.set('df_IW37nReporte',pickle.dumps(df_IW37nReporte))
        RedisDockers.set('df_IW39',pickle.dumps(df_IW39))
        RedisDockers.set('Process_Status_Data_SAP_Indicadores','completed')
        
        return ({
            "Message": "Oki Doki"
            })


@router.get('/GetDataSAPIw39FromRedis', tags=["Indicadores"])
def Get_Data_IW39_From_Redis():
    
    df_result = []
    pickled_IW39 =RedisDockers.get('df_IW39')
    df_IW39 = pickle.loads(pickled_IW39)
    function_return_Streaming(df_IW39,df_result)
    def generate():
        for chunk in df_result:
            yield from chunk
    return StreamingResponse(generate(), media_type='application/json')


@router.get('/GetDataSAPIw37nBaseFromRedis', tags=["Indicadores"])
def Get_Data_IW37nBase_From_Redis():
    
    df_result = []
    pickled_IW37nBase =RedisDockers.get('df_IW37nBase')
    df_IW37Base = pickle.loads(pickled_IW37nBase)
    function_return_Streaming(df_IW37Base,df_result)
    def generate():
        for chunk in df_result:
            yield from chunk
    return StreamingResponse(generate(), media_type='application/json')


@router.get('/GetDataSAPIw37nReporteFromRedis', tags=["Indicadores"])
def Get_Data_IW37nReporte_From_Redis():
    
    df_result = []
    pickled_IW37nReporte =RedisDockers.get('df_IW37nReporte')
    df_IW37Reporte = pickle.loads(pickled_IW37nReporte)
    function_return_Streaming(df_IW37Reporte,df_result)
    def generate():
        for chunk in df_result:
            yield from chunk
    return StreamingResponse(generate(), media_type='application/json')


@router.get('/GetDataSAPIw29FromRedis', tags=["Indicadores"])
def Get_Data_IW29_From_Redis():
    
    df_result = []
    pickled_IW29 =RedisDockers.get('df_IW29')
    df_IW29 = pickle.loads(pickled_IW29)
    function_return_Streaming(df_IW29,df_result)
    def generate():
        for chunk in df_result:
            yield from chunk
    return StreamingResponse(generate(), media_type='application/json')

#-----------------------------------------------------------------------


#Poceso de DataFrames para respuesta individual
async def Process_IW39 (type: Optional[str]=None):    
    
    All_Data_IW39 = []
    current_date, Semana, Anho = get_current_datetime()
        
    # if type is None:
    #     print("Obteniendo semanal IW39")
    #     CursorIW39 = db.iw39report.find({
    #         "Semana": str(Semana),
    #         "Anho": str(Anho)
    #     })
    # else:
    #     print("Obteniendo anual IW39")
    #     CursorIW39 = db.iw39report.find({
    #         "Anho": str(Anho)
    #     })
    
    def MongoDB_Anual():
        print("Obteniendo anual IW39")
        CursorIW39 = db.iw39report.find({
            "Anho": str(Anho)
        })
        return CursorIW39
        
    def MongoDB_Mensual():
        print("Obteniendo mensual IW39")
        CursorIW39 = db.iw39report.find({
            "Semana": {"$in": [str(Semana), str(Semana-1), str(Semana-2), str(Semana-3)],},
            "Anho": str(Anho)
        })
        return CursorIW39
        
    def MongoDB_Semanal():    
        print("Obteniendo semanal IW39")
        CursorIW39 = db.iw39report.find({
            "Semana": str(Semana),
            "Anho": str(Anho)   
        })
        
    switch = {
        "Anual": MongoDB_Anual,
        "Mensual": MongoDB_Mensual,
        "Semanal": MongoDB_Semanal
    }
    
    Result = switch.get(type, MongoDB_Semanal)()
    
    #await id_to_string_process(CursorIW39,All_Data_IW39)
    await id_to_string_process(Result,All_Data_IW39)
    df_IW39 = pd.DataFrame(All_Data_IW39)

    df_IW39["Orden-Semana"] = df_IW39["Orden"].astype(str) + "-" + df_IW39["Semana"].astype(str)
    df_IW39 = df_IW39[[
        "Orden",
        "Status del sistema",
        "Semana",
        "StatUsu",
        "P",
        "Ubicación técnica",
        "CpoClasif",
        "Texto breve",
        "PtoTrbRes",
        "Orden-Semana",
        ]]
    return df_IW39

async def Process_IW37nReporte (type: Optional[str]=None):
    All_Data_IW37nReporte = []
    current_date, Semana, Anho = get_current_datetime()

    # if type is None:
    #     print("Obteniendo semanal IW37nReporte de la semana: ", Semana)
    #     CursorIW37nReporte = db.iw37nreport.find({
    #         "Semana": str(Semana),
    #         "Anho": str(Anho)
    #     })
    # else:
    #     print("Obteniendo anual IW37nReporte")
    #     CursorIW37nReporte = db.iw37nreport.find({
    #         "Anho": str(Anho),  
    #     })
        
    def MongoDB_Anual():
        print("Obteniendo anual IW37nReporte")
        CursorIW37nReporte = db.iw37nreport.find({
            "Anho": str(Anho),  
        })
        return CursorIW37nReporte
    
    def MongoDB_Mensual():
        print("Obteniendo mensual IW37nReporte")
        CursorIW37nReporte = db.iw37nreport.find({
            "Semana": {"$in": [str(Semana), str(Semana-1), str(Semana-2), str(Semana-3)],},
            "Anho": str(Anho)
        })
        return CursorIW37nReporte
    
    def MongoDB_Semanal():
        print("Obteniendo semanal IW37nReporte")
        CursorIW37nReporte = db.iw37nreport.find({
            "Semana": str(Semana),
            "Anho": str(Anho)
        })
        return CursorIW37nReporte     
    
    switch = {
        "Anual": MongoDB_Anual,
        "Mensual": MongoDB_Mensual,
        "Semanal": MongoDB_Semanal
    }
    
    Result = switch.get(type, MongoDB_Semanal)()
    
    #await id_to_string_process(CursorIW37nReporte,All_Data_IW37nReporte)
    await id_to_string_process(Result,All_Data_IW37nReporte)
    df_IW37nReporte = pd.DataFrame(All_Data_IW37nReporte)

    df_IW37nReporte["Orden-Semana"] = df_IW37nReporte["Orden"].astype(str) + "-" + df_IW37nReporte["Semana"].astype(str)
    df_IW37nReporte["Inic.extr."] = pd.to_datetime(df_IW37nReporte["Inic.extr."].str.replace(".", "/"), format="%d/%m/%Y").dt.date
    df_IW37nReporte['Inic.extr.'] = df_IW37nReporte['Inic.extr.'].apply(lambda x: pd.to_datetime(x, unit='ms') if isinstance(x, (int, float)) else pd.to_datetime(x)).dt.strftime('%Y-%m-%dT%H:%M:%S')
    
    df_IW37nReporte = df_IW37nReporte[[
        "Orden",
        "Inic.extr.",
        "Stat.sist.",
        "Revisión",
        "Trbjo real",
        "StatUsu",
        "Semana",
        "Orden-Semana",
        "Ubic.técn.",
        "PtoTrbRes",
        " Trabajo",
        "Op.",
        "CpoClasif",
        "Texto breve",
        "P"
        ]]
    
    return df_IW37nReporte

async def Process_IW37nBase (type: Optional[str]=None):
    All_Data_IW37nBase = [] 
    current_date, Semana, Anho = get_current_datetime()
    
    # if type is None:
    #     print("Obteniendo semanal IW37nBase")
    #     CursorIW37nBase = db.iw37n.find({
    #         "Semana": str(Semana),
    #         "Anho": str(Anho)
    #     })
    # else:
    #     print("Obteniendo anual IW37nBase")
    #     CursorIW37nBase = db.iw37n.find({
    #         "Anho": str(Anho),
    #     })
    
    def MongoDB_Anual():
        print("Obteniendo anual IW37nBase")
        CursorIW37nBase = db.iw37n.find({
            "Anho": str(Anho),
        })
        return CursorIW37nBase
    
    def MongoDB_Mensual():
        print("Obteniendo mensual IW37nBase")
        CursorIW37nBase = db.iw37n.find({
            "Semana": {"$in": [str(Semana), str(Semana-1), str(Semana-2), str(Semana-3)],},
            "Anho": str(Anho)
        })
        return CursorIW37nBase
    
    def MongoDB_Semanal():
        print("Obteniendo semanal IW37nBase")
        CursorIW37nBase = db.iw37n.find({
            "Semana": str(Semana),
            "Anho": str(Anho)
        })
        return CursorIW37nBase
        
    switch = {
        "Anual": MongoDB_Anual,
        "Mensual": MongoDB_Mensual,
        "Semanal": MongoDB_Semanal
    }
    
    Result = switch.get(type, MongoDB_Semanal)()
    
    #await id_to_string_process(CursorIW37nBase,All_Data_IW37nBase)
    await id_to_string_process(Result,All_Data_IW37nBase)
    df_IW37nBase = pd.DataFrame(All_Data_IW37nBase)
        
    df_IW37nBase["Orden-Semana"] = df_IW37nBase["Orden"].astype(str) + "-" + df_IW37nBase["Semana"].astype(str)
    df_IW37nBase["Inic.extr."] = pd.to_datetime(df_IW37nBase["Inic.extr."].str.replace(".", "/"), format="%d/%m/%Y").dt.date
    df_IW37nBase['Inic.extr.'] = df_IW37nBase['Inic.extr.'].apply(lambda x: pd.to_datetime(x, unit='ms') if isinstance(x, (int, float)) else pd.to_datetime(x)).dt.strftime('%Y-%m-%dT%H:%M:%S')
    df_IW37nBase = df_IW37nBase[
        [
        'Orden',
        'Aviso',
        'Texto breve',
        'Op.',
        'Texto breve operación',
        'PtoTbjoOp',
        'Cl.',
        'Ubic.técn.',
        'Denomin.',
        'Autor',
        'Inic.extr.',
        'Fe.entrada',
        'CpoClasif',
        'P',
        'PtoTrbRes',
        'PstoTbjo',
        'StatSistOp',
        'Stat.sist.',
        'Revisión',
        'Trbjo real',
        ' Trabajo',
        'Inic.real',
        'StatUsu',
        'Semana',
        'Orden-Semana']
        ]
    return df_IW37nBase

async def Process_Condiciones ():
    All_Data_Condiciones = []

    CursorIndicadores = db.baseindicadores.find({})
    await id_to_string_process(CursorIndicadores,All_Data_Condiciones)
    df_Indicadores = pd.DataFrame(All_Data_Condiciones)
    
    return df_Indicadores

async def Process_IW37nBase_2 (type: Optional[str]=None):
    
    if type is None:
        print("Obteniendo semanal")
        df_IW37nBase = await Process_IW37nBase()
        Result_IW37nReporte = await Process_IW37nReporte()
        Result_Condiciones = await Process_Condiciones()
        Result_IW39 = await Process_IW39 ()
    else:
        print("Obteniendo anual")
        df_IW37nBase = await Process_IW37nBase(type="Total")
        Result_IW37nReporte = await Process_IW37nReporte(type="Total")
        Result_Condiciones = await Process_Condiciones()
        Result_IW39 = await Process_IW39 (type="Total")
    
    
    Result_IW39 = Result_IW39[["Status del sistema","Orden-Semana"]]
    df_IW37nBase = pd.merge(df_IW37nBase, Result_IW39, on='Orden-Semana',how='left')
    df_IW37nBase.rename(columns={'Status del sistema':'Status Sistema Reportado'}, inplace=True)
    
    Result_IW37nReporte = Result_IW37nReporte[["Trbjo real","StatUsu","Orden-Semana"]]
    df_IW37nBase = pd.merge(df_IW37nBase, Result_IW37nReporte, on='Orden-Semana',how='left')
    df_IW37nBase.rename(columns={'Trbjo real_x':'Trbjo real'}, inplace=True)
    df_IW37nBase.rename(columns={'StatUsu_x':'StatUsu'}, inplace=True)
    df_IW37nBase.rename(columns={'Trbjo real_y':'Trabajo Real'}, inplace=True)
    df_IW37nBase.rename(columns={'StatUsu_y':'Status Usuario'}, inplace=True)
    
    df_IW37nBase['Status Sistema Reportado'] = df_IW37nBase['Status Sistema Reportado'].str[:9]
    Result_Condiciones.rename(columns={'StatusSistema':'Status Sistema Reportado'}, inplace=True)
    df_IW37nBase = pd.merge(df_IW37nBase, Result_Condiciones[['Status Sistema Reportado','StatusKPI']], on='Status Sistema Reportado',how='left')
    df_IW37nBase["OTCerradas"] =  np.where(df_IW37nBase["StatusKPI"] == "Cerrado", df_IW37nBase["Orden"], 0)
    df_IW37nBase["UT"] = df_IW37nBase["Ubic.técn."].str[:13].str.strip()
    
    #Temporal
    #---------------------------------------------
    df_IW37nBase["UT2"] = df_IW37nBase["Ubic.técn."].str[9:13].str.strip()
    df_IW37nBase["Temp"] = np.where(df_IW37nBase["UT2"] == "SUBS", df_IW37nBase["Ubic.técn."], df_IW37nBase["UT"])
    df_IW37nBase["UT"] = df_IW37nBase["Temp"]
    df_IW37nBase.drop(columns=["Temp","UT2"], inplace=True)
    #---------------------------------------------
    
    df_IW37nBase = pd.merge(df_IW37nBase, Result_Condiciones[['UT','Area', 'SubArea']], on='UT',how='left')
    Result_Condiciones.rename(columns={'Ptotrabajo':'PtoTrbRes'}, inplace=True)
    df_IW37nBase = pd.merge(df_IW37nBase, Result_Condiciones[['PtoTrbRes','Denominacion', 'AreaResponsable', 'Empresa', 'TipoContrato']], on='PtoTrbRes',how='left')
        
    return df_IW37nBase

@router.get("/GetAndProcessIW37nBase_2", tags=["Indicadores"]) #Revisar ya que hace los mismo que la función de arriba
async def Process_IW37nBase_2 (type: Optional[str]=None):
    df_result = []  
    
    if type is None:
        print("Obteniendo semanal IW37nBase")
        df_IW37nBase = await Process_IW37nBase()
        Result_IW37nReporte = await Process_IW37nReporte()
        Result_Condiciones = await Process_Condiciones()
        Result_IW39 = await Process_IW39 ()
    else:
        print("Obteniendo anual IW37nBase")
        df_IW37nBase = await Process_IW37nBase(type="Total")
        Result_IW37nReporte = await Process_IW37nReporte(type="Total")
        Result_Condiciones = await Process_Condiciones()
        Result_IW39 = await Process_IW39 (type="Total")
    
    
    Result_IW39 = Result_IW39[["Status del sistema","Orden-Semana"]]
    df_IW37nBase = pd.merge(df_IW37nBase, Result_IW39, on='Orden-Semana',how='left')
    df_IW37nBase.rename(columns={'Status del sistema':'Status Sistema Reportado'}, inplace=True)
    
    Result_IW37nReporte = Result_IW37nReporte[["Trbjo real","StatUsu","Orden-Semana"]]
    df_IW37nBase = pd.merge(df_IW37nBase, Result_IW37nReporte, on='Orden-Semana',how='left')
    df_IW37nBase.rename(columns={'Trbjo real_x':'Trbjo real'}, inplace=True)
    df_IW37nBase.rename(columns={'StatUsu_x':'StatUsu'}, inplace=True)
    df_IW37nBase.rename(columns={'Trbjo real_y':'Trabajo Real'}, inplace=True)
    df_IW37nBase.rename(columns={'StatUsu_y':'Status Usuario'}, inplace=True)
    
    df_IW37nBase['Status Sistema Reportado'] = df_IW37nBase['Status Sistema Reportado'].str[:9]
    Result_Condiciones.rename(columns={'StatusSistema':'Status Sistema Reportado'}, inplace=True)
    df_IW37nBase = pd.merge(df_IW37nBase, Result_Condiciones[['Status Sistema Reportado','StatusKPI']], on='Status Sistema Reportado',how='left')
    df_IW37nBase["OTCerradas"] =  np.where(df_IW37nBase["StatusKPI"] == "Cerrado", df_IW37nBase["Orden"], 0)
    df_IW37nBase["UT"] = df_IW37nBase["Ubic.técn."].str[:13].str.strip()
    
    #Temporal
    #---------------------------------------------
    df_IW37nBase["UT2"] = df_IW37nBase["Ubic.técn."].str[9:13].str.strip()
    df_IW37nBase["Temp"] = np.where(df_IW37nBase["UT2"] == "SUBS", df_IW37nBase["Ubic.técn."], df_IW37nBase["UT"])
    df_IW37nBase["UT"] = df_IW37nBase["Temp"]
    df_IW37nBase.drop(columns=["Temp","UT2"], inplace=True)
    #---------------------------------------------
    
    df_IW37nBase = pd.merge(df_IW37nBase, Result_Condiciones[['UT','Area', 'SubArea']], on='UT',how='left')
    Result_Condiciones.rename(columns={'Ptotrabajo':'PtoTrbRes'}, inplace=True)
    df_IW37nBase = pd.merge(df_IW37nBase, Result_Condiciones[['PtoTrbRes','Denominacion', 'AreaResponsable', 'Empresa', 'TipoContrato']], on='PtoTrbRes',how='left')
    
    function_return_Streaming(df_IW37nBase,df_result)
    def generate():
        for chunk in df_result:
            yield from chunk
    return StreamingResponse(generate(), media_type='application/json')

async def Process_IW47 (type: Optional[str]=None):
    All_Data_IW47 = []
    current_year, Semana, Anho = get_current_datetime()
    if type is None:
        print("Obteniendo semanal IW47")
        CursorIW47 = db.iw47.find({
            "Semana": str(Semana),
            "Anho": str(Anho)
        })
        df_IW37nBase = await Process_IW37nBase()
        df_IW37nReporte = await Process_IW37nReporte()
        df_Condiciones = await Process_Condiciones()
        df_IW39 = await Process_IW39()
    else:
        print("Obteniendo anual IW47")
        CursorIW47 = db.iw47.find({
            "Anho": str(Anho)
        })
        df_IW37nBase = await Process_IW37nBase(type="Total")
        df_IW37nReporte = await Process_IW37nReporte(type="Total")
        df_Condiciones = await Process_Condiciones()
        df_IW39 = await Process_IW39(type="Total")
    
    await id_to_string_process(CursorIW47,All_Data_IW47)
        
    df_IW47 = pd.DataFrame(All_Data_IW47)
    df_IW47 = df_IW47.drop_duplicates()
    df_IW47 = pd.merge(df_IW47,df_IW37nBase[['Orden','Revisión']], on='Orden',how='left')

    df_IW47["RevisionIW47"] = "SEM" + df_IW47["Semana"].astype(str).str.zfill(2) + "-" + str(Anho)[-2:]
    df_IW47["Condicional"] = np.where(df_IW47["Revisión"] != df_IW47["RevisionIW47"], 1, 0)
    df_IW47 = df_IW47[df_IW47["Condicional"] == 1]
    
    df_IW47["Orden-Semana"] = df_IW47["Orden"].astype(str) + "-" + df_IW47["Semana"].astype(str)
    df_IW47 = pd.merge(df_IW47,df_IW39[["Orden-Semana","CpoClasif", "P", "PtoTrbRes", "Status del sistema", "StatUsu", "Texto breve", "Ubicación técnica"]], on='Orden-Semana',how='left')
    
    df_IW47.rename(columns={'P':'Prioridad'}, inplace=True)
    df_IW47.rename(columns={'Status del sistema':'Status Sistema Reportado'}, inplace=True)
    df_IW47["Status Sistema Reportado"] = df_IW47["Status Sistema Reportado"].str[:9].str.strip()
    df_IW47["Status Sistema Reportado"] = df_IW47["Status Sistema Reportado"].str[:9].str.strip()
    df_IW47.drop(columns=["Condicional","Revisión"], inplace=True)
    df_IW47.rename(columns={'RevisionIW47':'Revisión'}, inplace=True)   
    df_IW47 = df_IW47.drop_duplicates()
    df_IW47 = pd.merge(df_IW47,df_IW37nReporte[['Orden-Semana','Inic.extr.']], on='Orden-Semana',how='left')
    df_IW47 = df_IW47.drop_duplicates()

    df_IW47["UT"] = df_IW47["Ubicación técnica"].str[:13].str.strip()
    
    df_Condiciones.rename(columns={'StatusSistema':'Status Sistema Reportado'}, inplace=True)
    
    df_IW47 = pd.merge(df_IW47,df_Condiciones[['Status Sistema Reportado','StatusKPI']], on='Status Sistema Reportado',how='left')
    df_Condiciones.rename(columns={'Ptotrabajo':'PtoTrbRes'}, inplace=True)
    df_IW47 = pd.merge(df_IW47,df_Condiciones[['PtoTrbRes','Denominacion', 'AreaResponsable']], on='PtoTrbRes',how='left')
    df_IW47 = pd.merge(df_IW47,df_Condiciones[['UT','Area', 'SubArea']], on='UT',how='left')
    df_IW47 = df_IW47.drop_duplicates()
    
    df_IW47['Temp'] = df_IW47["Ubicación técnica"].fillna('').str.startswith('JP11-MI1').astype(int)
    df_IW47 = df_IW47[df_IW47['Temp'] == 0]
    df_IW47.drop(columns=["_id", "Temp"], inplace=True)
    df_IW47.rename(columns={'Inic.extr.':'InicioExtremo'}, inplace=True)
    df_IW47 = df_IW47.drop_duplicates(subset=['Orden'])
    
    df_IW37nReporte['Trbjo real'] = pd.to_numeric(df_IW37nReporte['Trbjo real'], errors='coerce')
    df_IW37nReporte = df_IW37nReporte.groupby(['Orden-Semana'], as_index=False)['Trbjo real'].sum()
    df_IW47 = pd.merge(df_IW47,df_IW37nReporte[['Orden-Semana','Trbjo real']], on='Orden-Semana',how='left')

    return df_IW47

async def Process_IW29 (type: Optional[str] = None):
    
    if type is None:
        All_Data_IW29 = []
        current_date, Semana, Anho = get_current_datetime()

        print("Obteniendo semanal IW29")
        CursorIW29 = db.iw29.find({
            "Semana": str(Semana),
            "Anho": str(Anho)
        })
    else:
        print("Obteniendo anual IW29")
        CursorIW29 = db.iw29.find({
            "Anho": str(Anho)
        })
    
    await id_to_string_process(CursorIW29,All_Data_IW29)

    df_IW29 = pd.DataFrame(All_Data_IW29)
        
    dataPrioridad = [
    {"Prioridad": "1", "DescripcionPrioridad": "E:Emergencia"},
    {"Prioridad": "2", "DescripcionPrioridad": "A:Alta"},
    {"Prioridad": "3", "DescripcionPrioridad": "B:Media"},
    {"Prioridad": "4", "DescripcionPrioridad": "C:Baja"},
    {"Prioridad": "5", "DescripcionPrioridad": "D:Muy Baja"},
    ]
    
    dataPrioridadResponsable =[
        {"PrioridadResponsable": "E:Emergencia-Mantto Mecánico-OXI Z. Seca", "Responsables": "Programador Mec. Óxidos"},
        {"PrioridadResponsable": "E:Emergencia-Mantto Mecánico-OXI Z. Húmeda", "Responsables": "Programador Mec. Óxidos"},
        {"PrioridadResponsable": "E:Emergencia-Confiabilidad-OXI Z. Seca", "Responsables": "Programador Mec. Óxidos"},
        {"PrioridadResponsable": "E:Emergencia-Confiabilidad-OXI Z. Húmeda", "Responsables": "Programador Mec. Óxidos"},
        {"PrioridadResponsable": "A:Alta-Mantto Mecánico-OXI Z. Seca", "Responsables": "Programador Mec. Óxidos"},
        {"PrioridadResponsable": "A:Alta-Mantto Mecánico-OXI Z. Húmeda", "Responsables": "Programador Mec. Óxidos"},
        {"PrioridadResponsable": "A:Alta-Confiabilidad-OXI Z. Seca", "Responsables": "Programador Mec. Óxidos"},
        {"PrioridadResponsable": "A:Alta-Confiabilidad-OXI Z. Húmeda", "Responsables": "Programador Mec. Óxidos"},
        {"PrioridadResponsable": "E:Emergencia-Mantto Mecánico-SUL Z. Seca", "Responsables": "Programador Mec. Sulfuros"},
        {"PrioridadResponsable": "E:Emergencia-Mantto Mecánico-SUL Z. Húmeda", "Responsables": "Programador Mec. Sulfuros"},
        {"PrioridadResponsable": "E:Emergencia-Mantto Mecánico-Truck Shop", "Responsables": "Programador Mec. Sulfuros"},
        {"PrioridadResponsable": "E:Emergencia-Confiabilidad-SUL Z. Seca", "Responsables": "Programador Mec. Sulfuros"},
        {"PrioridadResponsable": "E:Emergencia-Confiabilidad-SUL Z. Húmeda", "Responsables": "Programador Mec. Sulfuros"},
        {"PrioridadResponsable": "E:Emergencia-Confiabilidad-Truck Shop", "Responsables": "Programador Mec. Sulfuros"},
        {"PrioridadResponsable": "A:Alta-Mantto Mecánico-SUL Z. Seca", "Responsables": "Programador Mec. Sulfuros"},
        {"PrioridadResponsable": "A:Alta-Mantto Mecánico-SUL Z. Húmeda", "Responsables": "Programador Mec. Sulfuros"},
        {"PrioridadResponsable": "A:Alta-Mantto Mecánico-Truck Shop", "Responsables": "Programador Mec. Sulfuros"},
        {"PrioridadResponsable": "A:Alta-Confiabilidad-SUL Z. Seca", "Responsables": "Programador Mec. Sulfuros"},
        {"PrioridadResponsable": "A:Alta-Confiabilidad-SUL Z. Húmeda", "Responsables": "Programador Mec. Sulfuros"},
        {"PrioridadResponsable": "A:Alta-Confiabilidad-Truck Shop", "Responsables": "Programador Mec. Sulfuros"},
        {"PrioridadResponsable": "E:Emergencia-Mantto E & I-SUL Z. Seca", "Responsables": "Programador E&I"},
        {"PrioridadResponsable": "E:Emergencia-Mantto E & I-SUL Z. Húmeda", "Responsables": "Programador E&I"},
        {"PrioridadResponsable": "E:Emergencia-Mantto E & I-OXI Z. Seca", "Responsables": "Programador E&I"},
        {"PrioridadResponsable": "E:Emergencia-Mantto E & I-OXI Z. Húmeda", "Responsables": "Programador E&I"},
        {"PrioridadResponsable": "E:Emergencia-Mantto E & I-Truck Shop", "Responsables": "Programador E&I"},
        {"PrioridadResponsable": "E:Emergencia-Mantto E & I-Potencia", "Responsables": "Programador E&I"},
        {"PrioridadResponsable": "E:Emergencia-Mantto Potencia-SUL Z. Seca", "Responsables": "Programador E&I"},
        {"PrioridadResponsable": "E:Emergencia-Mantto Potencia-SUL Z. Húmeda", "Responsables": "Programador E&I"},
        {"PrioridadResponsable": "E:Emergencia-Mantto Potencia-OXI Z. Seca", "Responsables": "Programador E&I"},
        {"PrioridadResponsable": "E:Emergencia-Mantto Potencia-OXI Z. Húmeda", "Responsables": "Programador E&I"},
        {"PrioridadResponsable": "E:Emergencia-Mantto Potencia-Truck Shop", "Responsables": "Programador E&I"},
        {"PrioridadResponsable": "E:Emergencia-Mantto Potencia-Potencia", "Responsables": "Programador E&I"},
        {"PrioridadResponsable": "E:Emergencia-Confiabilidad-Potencia", "Responsables": "Programador E&I"},
        {"PrioridadResponsable": "A:Alta-Mantto E & I-SUL Z. Seca", "Responsables": "Programador E&I"},
        {"PrioridadResponsable": "A:Alta-Mantto E & I-SUL Z. Húmeda", "Responsables": "Programador E&I"},
        {"PrioridadResponsable": "A:Alta-Mantto E & I-OXI Z. Seca", "Responsables": "Programador E&I"},
        {"PrioridadResponsable": "A:Alta-Mantto E & I-OXI Z. Húmeda", "Responsables": "Programador E&I"},
        {"PrioridadResponsable": "A:Alta-Mantto E & I-Truck Shop", "Responsables": "Programador E&I"},
        {"PrioridadResponsable": "A:Alta-Mantto E & I-Potencia", "Responsables": "Programador E&I"},
        {"PrioridadResponsable": "A:Alta-Mantto Potencia-SUL Z. Seca", "Responsables": "Programador E&I"},
        {"PrioridadResponsable": "A:Alta-Mantto Potencia-SUL Z. Húmeda", "Responsables": "Programador E&I"},
        {"PrioridadResponsable": "A:Alta-Mantto Potencia-OXI Z. Seca", "Responsables": "Programador E&I"},
        {"PrioridadResponsable": "A:Alta-Mantto Potencia-OXI Z. Húmeda", "Responsables": "Programador E&I"},
        {"PrioridadResponsable": "A:Alta-Mantto Potencia-Truck Shop", "Responsables": "Programador E&I"},
        {"PrioridadResponsable": "A:Alta-Mantto Potencia-Potencia", "Responsables": "Programador E&I"},
        {"PrioridadResponsable": "A:Alta-Confiabilidad-Potencia", "Responsables": "Programador E&I"},
        {"PrioridadResponsable": "B:Media-Mantto Mecánico-OXI Z. Seca", "Responsables": "Planificador de Óxidos"},
        {"PrioridadResponsable": "B:Media-Confiabilidad-OXI Z. Seca", "Responsables": "Planificador de Óxidos"},
        {"PrioridadResponsable": "C:Baja-Mantto Mecánico-OXI Z. Seca", "Responsables": "Planificador de Óxidos"},
        {"PrioridadResponsable": "C:Baja-Confiabilidad-OXI Z. Seca", "Responsables": "Planificador de Óxidos"},
        {"PrioridadResponsable": "D:Muy Baja-Mantto Mecánico-OXI Z. Seca", "Responsables": "Planificador de Óxidos"},
        {"PrioridadResponsable": "D:Muy Baja-Confiabilidad-OXI Z. Seca", "Responsables": "Planificador de Óxidos"},
        {"PrioridadResponsable": "B:Media-Mantto Mecánico-OXI Z. Húmeda", "Responsables": "Planificador de Óxidos"},
        {"PrioridadResponsable": "B:Media-Confiabilidad-OXI Z. Húmeda", "Responsables": "Planificador de Óxidos"},
        {"PrioridadResponsable": "C:Baja-Mantto Mecánico-OXI Z. Húmeda", "Responsables": "Planificador de Óxidos"},
        {"PrioridadResponsable": "C:Baja-Confiabilidad-OXI Z. Húmeda", "Responsables": "Planificador de Óxidos"},
        {"PrioridadResponsable": "D:Muy Baja-Mantto Mecánico-OXI Z. Húmeda", "Responsables": "Planificador de Óxidos"},
        {"PrioridadResponsable": "D:Muy Baja-Confiabilidad-OXI Z. Húmeda", "Responsables": "Planificador de Óxidos"},
        {"PrioridadResponsable": "B:Media-Mantto Mecánico-SUL Z. Seca", "Responsables": "Planificador de Sulfuros"},
        {"PrioridadResponsable": "B:Media-Confiabilidad-SUL Z. Seca", "Responsables": "Planificador de Sulfuros"},
        {"PrioridadResponsable": "C:Baja-Mantto Mecánico-SUL Z. Seca", "Responsables": "Planificador de Sulfuros"},
        {"PrioridadResponsable": "C:Baja-Confiabilidad-SUL Z. Seca", "Responsables": "Planificador de Sulfuros"},
        {"PrioridadResponsable": "D:Muy Baja-Mantto Mecánico-SUL Z. Seca", "Responsables": "Planificador de Sulfuros"},
        {"PrioridadResponsable": "D:Muy Baja-Confiabilidad-SUL Z. Seca", "Responsables": "Planificador de Sulfuros"},
        {"PrioridadResponsable": "B:Media-Mantto Mecánico-SUL Z. Húmeda", "Responsables": "Planificador de Sulfuros"},
        {"PrioridadResponsable": "B:Media-Confiabilidad-SUL Z. Húmeda", "Responsables": "Planificador de Sulfuros"},
        {"PrioridadResponsable": "C:Baja-Mantto Mecánico-SUL Z. Húmeda", "Responsables": "Planificador de Sulfuros"},
        {"PrioridadResponsable": "C:Baja-Confiabilidad-SUL Z. Húmeda", "Responsables": "Planificador de Sulfuros"},
        {"PrioridadResponsable": "D:Muy Baja-Mantto Mecánico-SUL Z. Húmeda", "Responsables": "Planificador de Sulfuros"},
        {"PrioridadResponsable": "D:Muy Baja-Confiabilidad-SUL Z. Húmeda", "Responsables": "Planificador de Sulfuros"},
        {"PrioridadResponsable": "B:Media-Mantto E & I-SUL Z. Seca", "Responsables": "Planificador E&I"},
        {"PrioridadResponsable": "B:Media-Mantto E & I-SUL Z. Húmeda", "Responsables": "Planificador E&I"},
        {"PrioridadResponsable": "B:Media-Mantto E & I-OXI Z. Seca", "Responsables": "Planificador E&I"},
        {"PrioridadResponsable": "B:Media-Mantto E & I-OXI Z. Húmeda", "Responsables": "Planificador E&I"},
        {"PrioridadResponsable": "B:Media-Mantto E & I-Truck Shop", "Responsables": "Planificador E&I"},
        {"PrioridadResponsable": "C:Baja-Mantto E & I-SUL Z. Seca", "Responsables": "Planificador E&I"},
        {"PrioridadResponsable": "C:Baja-Mantto E & I-SUL Z. Húmeda", "Responsables": "Planificador E&I"},
        {"PrioridadResponsable": "C:Baja-Mantto E & I-OXI Z. Seca", "Responsables": "Planificador E&I"},
        {"PrioridadResponsable": "C:Baja-Mantto E & I-OXI Z. Húmeda", "Responsables": "Planificador E&I"},
        {"PrioridadResponsable": "C:Baja-Mantto E & I-Truck Shop", "Responsables": "Planificador E&I"},
        {"PrioridadResponsable": "D:Muy Baja-Mantto E & I-SUL Z. Seca", "Responsables": "Planificador E&I"},
        {"PrioridadResponsable": "D:Muy Baja-Mantto E & I-SUL Z. Húmeda", "Responsables": "Planificador E&I"},
        {"PrioridadResponsable": "D:Muy Baja-Mantto E & I-OXI Z. Seca", "Responsables": "Planificador E&I"},
        {"PrioridadResponsable": "D:Muy Baja-Mantto E & I-OXI Z. Húmeda", "Responsables": "Planificador E&I"},
        {"PrioridadResponsable": "D:Muy Baja-Mantto E & I-Truck Shop", "Responsables": "Planificador E&I"},
        {"PrioridadResponsable": "E:Emergencia-Confiabilidad-Puerto", "Responsables": "Planificador Mec. Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "E:Emergencia-Confiabilidad-Área 4000", "Responsables": "Planificador Mec. Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "E:Emergencia-Mantto Mecánico-Puerto", "Responsables": "Planificador Mec. Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "E:Emergencia-Mantto Mecánico-Área 4000", "Responsables": "Planificador Mec. Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "A:Alta-Confiabilidad-Puerto", "Responsables": "Planificador Mec. Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "A:Alta-Confiabilidad-Área 4000", "Responsables": "Planificador Mec. Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "A:Alta-Mantto Mecánico-Puerto", "Responsables": "Planificador Mec. Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "A:Alta-Mantto Mecánico-Área 4000", "Responsables": "Planificador Mec. Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "B:Media-Confiabilidad-Puerto", "Responsables": "Planificador Mec. Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "B:Media-Confiabilidad-Área 4000", "Responsables": "Planificador Mec. Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "B:Media-Confiabilidad-Potencia", "Responsables": "Planificador Mec. Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "B:Media-Mantto Mecánico-Puerto", "Responsables": "Planificador Mec. Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "B:Media-Mantto Mecánico-Área 4000", "Responsables": "Planificador Mec. Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "B:Media-Mantto Mecánico-Potencia", "Responsables": "Planificador Mec. Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "C:Baja-Confiabilidad-Puerto", "Responsables": "Planificador Mec. Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "C:Baja-Confiabilidad-Área 4000", "Responsables": "Planificador Mec. Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "C:Baja-Confiabilidad-Potencia", "Responsables": "Planificador Mec. Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "C:Baja-Mantto Mecánico-Puerto", "Responsables": "Planificador Mec. Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "C:Baja-Mantto Mecánico-Área 4000", "Responsables": "Planificador Mec. Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "C:Baja-Mantto Mecánico-Potencia", "Responsables": "Planificador Mec. Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "D:Muy Baja-Confiabilidad-Puerto", "Responsables": "Planificador Mec. Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "D:Muy Baja-Confiabilidad-Área 4000", "Responsables": "Planificador Mec. Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "D:Muy Baja-Confiabilidad-Potencia", "Responsables": "Planificador Mec. Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "D:Muy Baja-Mantto Mecánico-Puerto", "Responsables": "Planificador Mec. Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "D:Muy Baja-Mantto Mecánico-Área 4000", "Responsables": "Planificador Mec. Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "D:Muy Baja-Mantto Mecánico-Potencia", "Responsables": "Planificador Mec. Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "E:Emergencia-Mantto E & I-Puerto", "Responsables": "Planificador E&I Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "E:Emergencia-Mantto E & I-Área 4000", "Responsables": "Planificador E&I Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "E:Emergencia-Mantto Potencia-Puerto", "Responsables": "Planificador E&I Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "E:Emergencia-Mantto Potencia-Área 4000", "Responsables": "Planificador E&I Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "A:Alta-Mantto E & I-Puerto", "Responsables": "Planificador E&I Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "A:Alta-Mantto E & I-Área 4000", "Responsables": "Planificador E&I Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "A:Alta-Mantto Potencia-Puerto", "Responsables": "Planificador E&I Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "A:Alta-Mantto Potencia-Área 4000", "Responsables": "Planificador E&I Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "B:Media-Mantto E & I-Puerto", "Responsables": "Planificador E&I Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "B:Media-Mantto E & I-Área 4000", "Responsables": "Planificador E&I Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "B:Media-Mantto E & I-Potencia", "Responsables": "Planificador E&I Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "B:Media-Mantto Potencia-Puerto", "Responsables": "Planificador E&I Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "B:Media-Mantto Potencia-Área 4000", "Responsables": "Planificador E&I Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "B:Media-Mantto Potencia-Potencia", "Responsables": "Planificador E&I Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "C:Baja-Mantto E & I-Puerto", "Responsables": "Planificador E&I Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "C:Baja-Mantto E & I-Área 4000", "Responsables": "Planificador E&I Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "C:Baja-Mantto E & I-Potencia", "Responsables": "Planificador E&I Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "C:Baja-Mantto Potencia-Puerto", "Responsables": "Planificador E&I Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "C:Baja-Mantto Potencia-Área 4000", "Responsables": "Planificador E&I Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "C:Baja-Mantto Potencia-Potencia", "Responsables": "Planificador E&I Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "D:Muy Baja-Mantto E & I-Puerto", "Responsables": "Planificador E&I Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "D:Muy Baja-Mantto E & I-Área 4000", "Responsables": "Planificador E&I Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "D:Muy Baja-Mantto E & I-Potencia", "Responsables": "Planificador E&I Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "D:Muy Baja-Mantto Potencia-Puerto", "Responsables": "Planificador E&I Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "D:Muy Baja-Mantto Potencia-Área 4000", "Responsables": "Planificador E&I Puerto/Área 4000/Potencia"},
        {"PrioridadResponsable": "D:Muy Baja-Mantto Potencia-Potencia", "Responsables": "Planificador E&I Puerto/Área 4000/Potencia"},
    ]

    dataStatusSistemaAvisos = [
        {"Stat.sist.": "MDIF", "DescripcionStatus": "4. Avisos Abiertos"},
        {"Stat.sist.": "MEAB", "DescripcionStatus": "4. Avisos Abiertos"},
        {"Stat.sist.": "MECE", "DescripcionStatus": "5. Avisos Rechazados"},
        {"Stat.sist.": "MECE MIMP ORAS", "DescripcionStatus": "1. Avisos Ejecutados (OT cerrada)"},
        {"Stat.sist.": "MECE ORAS", "DescripcionStatus": "1. Avisos Ejecutados (OT cerrada)"},
        {"Stat.sist.": "MECE ORAS PTBO", "DescripcionStatus": "1. Avisos Ejecutados (OT cerrada)"},
        {"Stat.sist.": "MECE PTBO", "DescripcionStatus": "5. Avisos Rechazados"},
        {"Stat.sist.": "METR", "DescripcionStatus": "3. Avisos Aprobados (Liberado)"},
        {"Stat.sist.": "METR MIMP ORAS", "DescripcionStatus": "2. Avisos Tratados (Con OT)"},
        {"Stat.sist.": "METR ORAS PREI", "DescripcionStatus": "2. Avisos Tratados (Con OT)"},
        {"Stat.sist.": "METR ORAS", "DescripcionStatus": "2. Avisos Tratados (Con OT)"},
    ]
    
    dataResponsablePlaneamiento = [
        {"PosicionPlaneamiento": "Programador Mec. Óxidos", "Responsables": "Nelly Morales/Cliver Cari"},
        {"PosicionPlaneamiento": "Programador Mec. Sulfuros", "Responsables": "Dayana Miranda/Tatiana Hualla"},
        {"PosicionPlaneamiento": "Programador E&I", "Responsables": "Erick Ramos/Joel Valentin"},
        {"PosicionPlaneamiento": "Planificador de Óxidos", "Responsables": "Jean Espinoza/Jorge Rodriguez"},
        {"PosicionPlaneamiento": "Planificador de Sulfuros", "Responsables": "Johan Callomamani/Renato Alarcon"},
        {"PosicionPlaneamiento": "Planificador E&I", "Responsables": "Jhon Quispe/Pedro Milachay"},
        {"PosicionPlaneamiento": "Planificador Mec. Puerto/Área 4000/Potencia", "Responsables": "Jorge Ramirez/Sadin Valdivia"},
        {"PosicionPlaneamiento": "Planificador E&I Puerto/Área 4000/Potencia", "Responsables": "Jorge Ramirez/Sadin Valdivia"},
    ]
    
    dataResponsableEjecucionAvisos = [
    {"Area": "ENERGÍA", "AreaResponsable": "Mantto E & I", "ResponsableEjecucion": "Andy Cabrera"},
    {"Area": "ENERGÍA", "AreaResponsable": "Confiabilidad", "ResponsableEjecucion": "Carlos Bardales/Juan Huete"},
    {"Area": "ENERGÍA", "AreaResponsable": "Mantto Potencia", "ResponsableEjecucion": "Andy Cabrera"},
    {"Area": "INFRAESTRUCTURA", "AreaResponsable": "Mantto E & I", "ResponsableEjecucion": "Jorge F / Juan Salazar"},
    {"Area": "INFRAESTRUCTURA", "AreaResponsable": "Mantto Mecánico", "ResponsableEjecucion": "Herbert Yuto"},
    {"Area": "ÓXIDOS", "AreaResponsable": "Mantto E & I", "ResponsableEjecucion": "Jorge F / Juan Salazar"},
    {"Area": "ÓXIDOS", "AreaResponsable": "Mantto Mecánico", "ResponsableEjecucion": "Eduardo Bernahola"},
    {"Area": "ÓXIDOS", "AreaResponsable": "Confiabilidad", "ResponsableEjecucion": "Carlos Bardales/Juan Huete"},
    {"Area": "PUERTO", "AreaResponsable": "Mantto E & I", "ResponsableEjecucion": "Jorge F / Juan Salazar"},
    {"Area": "PUERTO", "AreaResponsable": "Mantto Mecánico", "ResponsableEjecucion": "Wiles Chavez/ Herbert Yuto"},
    {"Area": "SULFUROS", "AreaResponsable": "Mantto Mecánico", "ResponsableEjecucion": "Martin Castro/ Jimmy Solano"},
    {"Area": "SULFUROS", "AreaResponsable": "Mantto E & I", "ResponsableEjecucion": "Jorge F / Juan Salazar"},
    {"Area": "SULFUROS", "AreaResponsable": "Mantto Potencia", "ResponsableEjecucion": "Andy Cabrera"},
    {"Area": "SULFUROS", "AreaResponsable": "Confiabilidad", "ResponsableEjecucion": "Carlos Bardales/Juan Huete"},
    {"Area": "ENERGÍA", "AreaResponsable": "Termofusión", "ResponsableEjecucion": "Wiles Chavez"},
    {"Area": "INFRAESTRUCTURA", "AreaResponsable": "Termofusión", "ResponsableEjecucion": "Wiles Chavez"},
    {"Area": "ÓXIDOS", "AreaResponsable": "Termofusión", "ResponsableEjecucion": "Wiles Chavez"},
    {"Area": "PUERTO", "AreaResponsable": "Termofusión", "ResponsableEjecucion": "Wiles Chavez"},
    {"Area": "SULFUROS", "AreaResponsable": "Termofusión", "ResponsableEjecucion": "Wiles Chavez"},
    ]
    
    df_Prioridad = pd.DataFrame(dataPrioridad)
    df_dataPrioridadResponsable = pd.DataFrame(dataPrioridadResponsable)
    df_dataStatusSistemaAvisos = pd.DataFrame(dataStatusSistemaAvisos)
    df_dataResponsablePlaneamiento = pd.DataFrame(dataResponsablePlaneamiento)
    df_dataResponsableEjecucionAvisos = pd.DataFrame(dataResponsableEjecucionAvisos)
    df_Condiciones = await Process_Condiciones()
    
    df_IW29["Creado el"] = pd.to_datetime(df_IW29["Creado el"].str.replace(".", "/"), format="%d/%m/%Y").dt.date
    df_IW29['Creado el'] = df_IW29['Creado el'].apply(lambda x: pd.to_datetime(x, unit='ms') if isinstance(x, (int, float)) else pd.to_datetime(x)).dt.strftime('%Y-%m-%dT%H:%M:%S')
    df_IW29 = df_IW29[df_IW29["Cl."] != "Z3"]
    df_IW29.rename(columns={'P':'Prioridad'}, inplace=True)
    df_IW29 = pd.merge(df_IW29,df_Prioridad, on='Prioridad',how='left')
    df_IW29["UT"] = df_IW29["Ubicac.técnica"].str[:13].str.strip()
    df_IW29 = pd.merge(df_IW29,df_Condiciones[['UT','Area', 'SubArea']], on='UT',how='left')
    
    df_Condiciones.rename(columns={'Ptotrabajo':'PtoTrbRes'}, inplace=True)
    df_IW29 = pd.merge(df_IW29,df_Condiciones[['PtoTrbRes', 'AreaResponsable']], on='PtoTrbRes',how='left')
    df_IW29["PrioridadResponsable"] = df_IW29["DescripcionPrioridad"].astype(str) + "-" + df_IW29["AreaResponsable"].astype(str) + "-" + df_IW29["SubArea"].astype(str)
    df_IW29 = pd.merge(df_IW29,df_dataPrioridadResponsable[['PrioridadResponsable','Responsables']], on='PrioridadResponsable',how='left')
    df_IW29 = pd.merge(df_IW29,df_dataStatusSistemaAvisos[['Stat.sist.','DescripcionStatus']], on='Stat.sist.',how='left')
    df_IW29.rename(columns={'Responsables':'PosicionPlaneamiento'}, inplace=True)
    df_IW29 = pd.merge(df_IW29,df_dataResponsablePlaneamiento[['PosicionPlaneamiento','Responsables']], on='PosicionPlaneamiento',how='left')
    df_IW29 = pd.merge(df_IW29,df_dataResponsableEjecucionAvisos[['Area','AreaResponsable','ResponsableEjecucion']], on=['Area', 'AreaResponsable'],how='left')
    df_IW29 = df_IW29.drop_duplicates()
    df_IW29 = df_IW29.drop(columns=["Fecha"])
    df_IW29.rename(columns={'Creado el':'Fecha'}, inplace=True)
    
    return df_IW29


#-----------------------------------------------------------------------



#Proceso de Tratamiento y respuesta a cliente Individual
@router.get("/GetAndProcessIW39", tags=["Indicadores"])
async def Get_Process_IW39 ():
    df_result = []  
        
    df_IW39 = await Process_IW39()
    
    function_return_Streaming(df_IW39,df_result)
    def generate():
        for chunk in df_result:
            yield from chunk
    return StreamingResponse(generate(), media_type='application/json')

@router.get("/GetAndProcessIW37nReporte", tags=["Indicadores"])
async def Get_Process_IW37nReporte ():
    df_result = []  
    
    df_IW37nReporte = await Process_IW37nReporte()
    
    function_return_Streaming(df_IW37nReporte,df_result)
    def generate():
        for chunk in df_result:
            yield from chunk
    return StreamingResponse(generate(), media_type='application/json')

@router.get("/GetAndProcessIW37nBase", tags=["Indicadores"])
async def Get_Process_IW37nBase (): #Get_Process_IW37nReporte
    df_result = []  
    
    df_IW37nBase = await Process_IW37nBase_2()
    
    function_return_Streaming(df_IW37nBase,df_result)
    def generate():
        for chunk in df_result:
            yield from chunk
    return StreamingResponse(generate(), media_type='application/json')

@router.get("/GetAndProcessIW47", tags=["Indicadores"])
async def Get_Process_IW47 ():
    df_result = []  
    
    df_IW47 = await Process_IW47()
    
    function_return_Streaming(df_IW47,df_result)
    def generate():
        for chunk in df_result:
            yield from chunk
    return StreamingResponse(generate(), media_type='application/json')

@router.get("/GetAndProcessIW29", tags=["Indicadores"])
async def Get_Process_IW29 ():
    df_result = []  
    
    df_IW29 = await Process_IW29()

    function_return_Streaming(df_IW29,df_result)
    def generate():
        for chunk in df_result:
            yield from chunk
    return StreamingResponse(generate(), media_type='application/json')

#-----------------------------------------------------------------------



#Poceso de DataFrames para respuesta desde Redis

@router.get("/GetAndProcessIW37nBase_3", tags=["Indicadores"])
async def Process_IW37nBase_3(ype: Optional[str]=None):

    df_IW37nBase = await Process_IW37nBase()
    Result_IW37nReporte = await Process_IW37nReporte()
    Result_Condiciones = await Process_Condiciones_2()
    Result_IW39 = await Process_IW39 ()

    Result_IW39 = Result_IW39[["Status del sistema","Orden-Semana"]]
    df_IW37nBase = pd.merge(df_IW37nBase, Result_IW39, on='Orden-Semana',how='left')
    df_IW37nBase.rename(columns={'Status del sistema':'Status Sistema Reportado'}, inplace=True)

    Result_IW37nReporte = Result_IW37nReporte[["Trbjo real","StatUsu","Orden-Semana"]]
    df_IW37nBase = pd.merge(df_IW37nBase, Result_IW37nReporte, on='Orden-Semana',how='left')
    df_IW37nBase.rename(columns={'Trbjo real_x':'Trbjo real'}, inplace=True)
    df_IW37nBase.rename(columns={'StatUsu_x':'StatUsu'}, inplace=True)
    df_IW37nBase.rename(columns={'Trbjo real_y':'Trabajo Real'}, inplace=True)
    df_IW37nBase.rename(columns={'StatUsu_y':'Status Usuario'}, inplace=True)

    df_IW37nBase = pd.merge(df_IW37nBase, Result_Condiciones[['PtoTrbRes','Denominacion', 'AreaResponsable', 'Empresa', 'TipoContrato']], on='PtoTrbRes',how='left')
    df_IW37nBase['Status Sistema Reportado'] = df_IW37nBase['Status Sistema Reportado'].str[:9]
    Result_Condiciones.rename(columns={'StatusSistema':'Status Sistema Reportado'}, inplace=True)
    df_IW37nBase = pd.merge(df_IW37nBase, Result_Condiciones[['Status Sistema Reportado','StatusKPI']], on='Status Sistema Reportado',how='left')
    df_IW37nBase["OTCerradas"] =  np.where(df_IW37nBase["StatusKPI"] == "Cerrado", df_IW37nBase["Orden"], 0)
    
    #Eliminando Mina
    df_IW37nBase['Seccion'] = df_IW37nBase['Ubic.técn.'].apply(lambda x: x.split('-')[1][:3])
    df_IW37nBase = df_IW37nBase[df_IW37nBase['Seccion'] != 'MI1']
    df_IW37nBase = df_IW37nBase.drop(columns=['Seccion'])

    df_IW37nBase["UT"] = df_IW37nBase["Ubic.técn."].str[:13].str.strip()
    df_IW37nBase["Concatenacion"] = df_IW37nBase["Ubic.técn."].str.cat(df_IW37nBase["PtoTrbRes"], sep="-")
    df_IW37nBase["Concat"] = df_IW37nBase["UT"].str.cat(df_IW37nBase["PtoTrbRes"], sep="-")
    df_IW37nBase = pd.merge(df_IW37nBase, Result_Condiciones[['Concatenacion','Area', 'SubArea']], on='Concatenacion',how='left')
    df_IW37nBase['Vacío'] = df_IW37nBase.apply(lambda row: 'Sí' if pd.isnull(row['Area']) and pd.isnull(row['SubArea']) else 'No', axis=1)
    
    #merge con condicional
    df_vacios = df_IW37nBase[df_IW37nBase['Area'].isnull() | df_IW37nBase['SubArea'].isnull()]
    result_unico = Result_Condiciones.drop_duplicates(subset='Concat', keep='first')
    df_actualizar = pd.merge(df_vacios,result_unico[['Concat', 'Area', 'SubArea']],on='Concat', how='left',suffixes=('', '_nuevo'))

    df_actualizar['Area'] = df_actualizar.apply(lambda row: row['Area_nuevo'] if pd.isnull(row['Area']) else row['Area'], axis=1)
    df_actualizar['SubArea'] = df_actualizar.apply(lambda row: row['SubArea_nuevo'] if pd.isnull(row['SubArea']) else row['SubArea'], axis=1)
    df_actualizar = df_actualizar.drop(columns=['Area_nuevo', 'SubArea_nuevo'])

    #Elimo en la principal los Area y subarea vacios
    df_IW37nBase = df_IW37nBase.dropna(subset=['Area', 'SubArea'])
    df_IW37nBase = pd.concat([df_IW37nBase, df_actualizar])
    df_IW37nBase = df_IW37nBase.drop_duplicates()

    return df_IW37nBase

@router.get("/Process_Condiciones_2", tags=["Indicadores"])
async def Process_Condiciones_2 ():
    df_result = []  
    All_Data_Condiciones2 = []

    CursorIndicadores_2 = db.Condicion2.find({}) #Deber ser una sola tabla de condiciones, borrar la antigua y reemplazar por esta
    
    await id_to_string_process(CursorIndicadores_2,All_Data_Condiciones2)
    df_Indicadores2 = pd.DataFrame(All_Data_Condiciones2)
    df_Indicadores2["Concatenacion"]= df_Indicadores2["UbicacionTecnica"].str.cat(df_Indicadores2["Ptotrabajo"], sep="-")
    df_Indicadores2["Concatenacion"] = df_Indicadores2["Concatenacion"].fillna("-")
    df_Indicadores2["Concat"]= df_Indicadores2["UT"].str.cat(df_Indicadores2["Ptotrabajo"], sep="-")
    df_Indicadores2["Concat"] = df_Indicadores2["Concat"].fillna("-")
    df_Indicadores2 = df_Indicadores2.drop_duplicates()

    return df_Indicadores2

@router.get("/backlog", tags=["Indicadores"])
async def Process_Backlog (): #Process_Baklog
    df_result = []	

    Result_IW37nReporte = await Process_IW37nReporte()
    Result_Condiciones = await Process_Condiciones_2()
    Result_Criticidad = await Process_CriticidadEquipos()

    Result_IW37nReporte['UT'] = Result_IW37nReporte['Ubic.técn.'].str[:13]
    categoria_extract = Result_IW37nReporte['UT'].str[5:8]
    Result_IW37nReporte['Categoria'] = np.where(categoria_extract == 'MI1', 'Mina', 'Planta')
    Result_IW37nReporte = Result_IW37nReporte[Result_IW37nReporte['Categoria'] != 'Mina']

    #Columna Status del sistema
    Result_IW37nReporte['Status del sistema'] = Result_IW37nReporte['Stat.sist.'].str[:9]
    Result_IW37nReporte = pd.merge(Result_IW37nReporte,Result_Condiciones[['StatusSistema','StatusKPI']],left_on='Status del sistema',right_on='StatusSistema',how='left')
    Result_IW37nReporte = Result_IW37nReporte[Result_IW37nReporte['StatusKPI'] == 'Pendiente de cierre'] #Revisar si hay algun otro status que se deba considerar
    Result_IW37nReporte = pd.merge(Result_IW37nReporte,Result_Condiciones[['PtoTrbRes','Denominacion']],left_on='PtoTrbRes',right_on='PtoTrbRes',how='left')

    Result_IW37nReporte["Concatenacion"] = Result_IW37nReporte["Ubic.técn."].str.cat(Result_IW37nReporte["PtoTrbRes"], sep="-") # Concatenación Ubic.técn.-PtoTrabajo - Mejorar lógica (UT completo)
    Result_IW37nReporte["Concat"] = Result_IW37nReporte["UT"].str.cat(Result_IW37nReporte["PtoTrbRes"], sep="-") # Concatenación UT-PtoTrabajo - Mejorar lógica (UT son los primeros 13 caracteres de Ubic.técn.)
    
    Result_IW37nReporte = pd.merge(Result_IW37nReporte, Result_Condiciones[['Concatenacion','Area', 'SubArea']], on='Concatenacion',how='left')
    df_vacios = Result_IW37nReporte[Result_IW37nReporte['Area'].isnull() | Result_IW37nReporte['SubArea'].isnull()]
    result_unico = Result_Condiciones.drop_duplicates(subset='Concat', keep='first')

    df_actualizar = pd.merge(df_vacios,result_unico[['Concat', 'Area', 'SubArea']],on='Concat', how='left',suffixes=('', '_nuevo')) # Trabajar con lambda
    df_actualizar['Area'] = df_actualizar.apply(lambda row: row['Area_nuevo'] if pd.isnull(row['Area']) else row['Area'], axis=1)
    df_actualizar['SubArea'] = df_actualizar.apply(lambda row: row['SubArea_nuevo'] if pd.isnull(row['SubArea']) else row['SubArea'], axis=1)
    df_actualizar = df_actualizar.drop(columns=['Area_nuevo', 'SubArea_nuevo'])

    Result_IW37nReporte = Result_IW37nReporte.dropna(subset=['Area', 'SubArea'])

    Result_IW37nReporte = pd.concat([Result_IW37nReporte, df_actualizar]) # Mejorar nombres
    Result_IW37nReporte = Result_IW37nReporte.drop_duplicates()

    #Columna TrabajoHoras
    Result_IW37nReporte['TrabajoHoras'] = pd.to_numeric(Result_IW37nReporte[' Trabajo'], errors='coerce').fillna(0)

    #Categoria Backlog
    statusu_extract = Result_IW37nReporte['StatUsu'].str[:4]
    conditions = [
        (statusu_extract == 'EJEC'),
        (statusu_extract == 'ESPL'),
        (statusu_extract == 'PLAN'),
        (statusu_extract == 'PLOK'),
        (statusu_extract == 'PROG'),
        (statusu_extract == 'REPR'),
    ]
    choices = ['-', 'Planificación', 'Planificación', 'Programación', 'Programación','Programación']
    Result_IW37nReporte['CategoriaBacklog'] = np.select(conditions, choices, default='-')

    #Quitando filas con categoria backlog = -
    Result_IW37nReporte = Result_IW37nReporte[Result_IW37nReporte['CategoriaBacklog'] != '-']

    Result_IW37nReporte  = pd.merge(Result_IW37nReporte,Result_Condiciones[['PtoTrbRes','Empresa']],left_on='PtoTrbRes',right_on='PtoTrbRes',how='left')
    Result_IW37nReporte = pd.merge(Result_IW37nReporte,Result_Condiciones[['PtoTrbRes','AreaResponsable']],left_on='PtoTrbRes',right_on='PtoTrbRes',how='left')
    Result_IW37nReporte['Trabajo_sum'] = Result_IW37nReporte.groupby(['Orden', 'Semana', 'Op.'])['TrabajoHoras'].transform('sum')
    Result_IW37nReporte.rename(columns={'CpoClasif':'TAG'}, inplace=True)

    Result_Criticidad = Result_Criticidad.drop_duplicates(subset='TAG') # Hay duplicados en criticidad, limpiar la data en mongoDB
    df_TAG = Result_IW37nReporte[Result_IW37nReporte['TAG'].notna()].copy()
    df_UT = Result_IW37nReporte[(Result_IW37nReporte['TAG'].isna()) | (Result_IW37nReporte['TAG'] == '')].copy() # Optimizar esto

    df_TAG = pd.merge(df_TAG,Result_Criticidad[['TAG','Criticidad']],on='TAG',how='left')
    df_UT = pd.merge(df_UT,Result_Criticidad[['UT','Criticidad']],left_on='Ubic.técn.',right_on='UT',how='left')
    Result_IW37nReporte = pd.concat([df_TAG,df_UT])

    return Result_IW37nReporte

@router.get("/GetAndProcessHHDisponibles", tags=["Indicadores"])
async def Get_Process_HHDisponibles ():
    df_result = []  
    
    df_HHDisponibles = await Process_HHDisponibles()
    
    function_return_Streaming(df_HHDisponibles,df_result)
    def generate():
        for chunk in df_result:
            yield from chunk
    return StreamingResponse(generate(), media_type='application/json')

async def Process_HHDisponibles ():
    All_Data_HHDisponibles = []
    CursorIndicadores = db.Backlog.find({})
    
    await id_to_string_process(CursorIndicadores,All_Data_HHDisponibles)
    df_HHDisponibles = pd.DataFrame(All_Data_HHDisponibles)
    
    return df_HHDisponibles

@router.get("/GetAndProcessCriticidadEquipos", tags=["Indicadores"])
async def Get_Process_Criticidad ():
    df_result = []  
    
    df_CriticidadEquipos = await Process_CriticidadEquipos()
    
    function_return_Streaming(df_CriticidadEquipos,df_result)
    def generate():
        for chunk in df_result:
            yield from chunk
    return StreamingResponse(generate(), media_type='application/json')

async def Process_CriticidadEquipos ():

    All_Data_CriticidadEquipos = []
    CursorIndicadores = db.CriticidadEquipos.find({})
    
    await id_to_string_process(CursorIndicadores,All_Data_CriticidadEquipos)
    df_CriticidadEquipos = pd.DataFrame(All_Data_CriticidadEquipos)
    
    return df_CriticidadEquipos

#-----------------------------------------------------------------------


#Redis To Client

@router.post("/UpdateDataIndicadoresToRedis", tags=["Indicadores"])
async def Update_Data_Indicadores_To_Redis ():

    #Proceso Semanal
    print("Procesando Backlog")
    df_Backlog = await Process_Backlog()
    print("Procesando HH Disponibles")
    df_HHDisponibles = await Process_HHDisponibles()
    print("Procesando Criticidad Equipos")
    df_Criticidad = await Process_CriticidadEquipos()
    print("Procesando IW29")
    df_IW29 = await Process_IW29()
    print("Procesando IW39")
    df_IW39 = await Process_IW39()
    print("Procesando IW37nReporte")
    df_IW37nReporte = await Process_IW37nReporte()
    print("Procesando IW37nBase")
    df_IW37nBase = await Process_IW37nBase_3()
    print("Procesando IW47")
    df_IW47 = await Process_IW47()
    
    
    #Proceso Anual
    # df_IW39 = await Process_IW39("Total")
    # df_IW37nReporte = await Process_IW37nReporte("Total")
    # df_IW37nBase = await Process_IW37nBase_2("Total")
    # df_IW47 = await Process_IW47("Total")
    
    #Proceso en Redis
    
    Process_Status_Data_Indicadores = RedisDockers.get('Process_Status_Data_Indicadores')
    print(Process_Status_Data_Indicadores)
    
    if Process_Status_Data_Indicadores is None or Process_Status_Data_Indicadores.decode('utf-8') != 'in progess':
        
        RedisDockers.set('Process_Status_Data_Indicadores','in progess')
        
        RedisDockers.set('df_IW29',pickle.dumps(df_IW29))
        df = pickle.loads(RedisDockers.get('df_IW29'))
        RedisDockers.set('df_IW39',pickle.dumps(df_IW39))
        RedisDockers.set('df_IW37nReporte',pickle.dumps(df_IW37nReporte))
        RedisDockers.set('df_IW37nBase',pickle.dumps(df_IW37nBase))
        RedisDockers.set('df_IW47',pickle.dumps(df_IW47))
        RedisDockers.set('df_Backlog',pickle.dumps(df_Backlog))
        RedisDockers.set('df_HHDisponibles',pickle.dumps(df_HHDisponibles))
        RedisDockers.set('df_Criticidad',pickle.dumps(df_Criticidad))
        RedisDockers.set('Process_Status_Data_Indicadores','completed')
        
        return ({
            "Message": "Oki Doki"
            })
    
@router.get('/GetDataIndicadoresFromRedis', tags=["Indicadores"])
async def Get_Data_Indicadores_From_Redis():
    
    print("Obteniendo datos de indicadores de redis")
    
    pickled_IW29 = RedisDockers.get('df_IW29')
    pickled_IW39 = RedisDockers.get('df_IW39')
    pickled_IW37nReporte = RedisDockers.get('df_IW37nReporte')
    pickled_IW37nBase = RedisDockers.get('df_IW37nBase')
    pickled_IW47 = RedisDockers.get('df_IW47')
    pickled_Backlog = RedisDockers.get('df_Backlog')
    pickled_HHDisponibles = RedisDockers.get('df_HHDisponibles')
    pickle_Criticidad = RedisDockers.get('df_Criticidad')

    df_IW29 = pickle.loads(pickled_IW29)
    df_IW39 = pickle.loads(pickled_IW39)
    df_IW37nReporte = pickle.loads(pickled_IW37nReporte)
    df_IW37nBase = pickle.loads(pickled_IW37nBase)
    df_IW47 = pickle.loads(pickled_IW47)
    df_Backlog = pickle.loads(pickled_Backlog)
    df_HHDisponibles = pickle.loads(pickled_HHDisponibles)
    df_Criticidad = pickle.loads(pickle_Criticidad)
    
    df_IW29 = df_IW29.where(pd.notnull(df_IW29), '')
    df_IW39 = df_IW39.where(pd.notnull(df_IW39), '')
    df_IW37nReporte = df_IW37nReporte.where(pd.notnull(df_IW37nReporte), '')
    df_IW37nBase = df_IW37nBase.where(pd.notnull(df_IW37nBase), '')
    df_IW47 = df_IW47.where(pd.notnull(df_IW47), '')
    df_Backlog = df_Backlog.where(pd.notnull(df_Backlog), '')
    df_HHDisponibles = df_HHDisponibles.where(pd.notnull(df_HHDisponibles), '')
    df_Criticidad = df_Criticidad.where(pd.notnull(df_Criticidad), '') 
    
    data_IW29 = df_IW29.to_dict(orient='records')
    data_IW39 = df_IW39.to_dict(orient='records')
    data_IW37nReporte = df_IW37nReporte.to_dict(orient='records')
    data_IW37nBase = df_IW37nBase.to_dict(orient='records')
    data_IW47 = df_IW47.to_dict(orient='records')
    data_Backlog = df_Backlog.to_dict(orient='records')
    data_HHDisponibles = df_HHDisponibles.to_dict(orient='records')
    data_Criticidad = df_Criticidad.to_dict(orient='records')
    
    
    print("Finalizando el proceso de Obteniendo datos de Indicadores desde redis")
    
    return {
        "data_IW29": data_IW29,
        "data_IW39": data_IW39,
        "data_IW37nReporte": data_IW37nReporte,
        "data_IW37nBase": data_IW37nBase,
        "data_IW47": data_IW47,
        "data_Backlog": data_Backlog,
        "data_HHDisponibles":  data_HHDisponibles,
        "data_Criticidad": data_Criticidad
    }

@router.post("/ResetProcessStatusRedis", tags=["Indicadores"])
async def Reset_Process_Status_Redis ():
    RedisDockers.set('Process_Status_Data_Indicadores','completed')
    return {
            "Message": "Oki Doki"
    }

@router.post("/borrarredis", tags=["Indicadores"])
async def BorrarRedis ():
    RedisDockers.delete('df_IW29')
    RedisDockers.delete('df_IW39')
    RedisDockers.delete('df_IW37nReporte')
    RedisDockers.delete('df_IW37nBase')
    RedisDockers.delete('df_IW47')
    return {
        "Message": "Oki Doki"
    }

#------------------------------------------------------------------------


#########################################################################
#PLAN MENSUAL
#########################################################################


async def Process_PlanMensual (type: Optional[str]=None):

    All_Data_IW37nBaseMensual = []
    All_Data_IW37nReport = []
    df_result = []  

    current_date, Semana, Anho = get_current_datetime()
    if type is None:
        print("Obteniendo Mensual iwbase")
        CursorIW37nBaseMensual = db.iw37nbaseMes.find({
            "Semana": "18",
            "Anho": str(Anho)
        })
    else:
        print("Obteniendo anual iwbase")
        CursorIW37nBaseMensual = db.iw37nbaseMes.find({
            "Anho": str(Anho),
        })

    await id_to_string_process(CursorIW37nBaseMensual,All_Data_IW37nBaseMensual)
    df_IW37nBase = pd.DataFrame(All_Data_IW37nBaseMensual)

    Result_IW37nReporte = await Process_IW37nReporte()
    Result_Condiciones = await Process_Condiciones_2()
    Result_IW39 = await Process_IW39 ()

    Result_IW39 = Result_IW39[["Status del sistema","Orden-Semana"]]
    df_IW37nBase["Orden-Semana"] = df_IW37nBase["Orden"].astype(str) + "-" + df_IW37nBase["Semana"].astype(str)
    df_IW37nBase = pd.merge(df_IW37nBase, Result_IW39, on='Orden-Semana',how='left')

    df_IW37nBase.rename(columns={'Status del sistema':'Status Sistema Reportado'}, inplace=True)
    df_IW37nBase = df_IW37nBase.drop_duplicates(subset=['Orden'], keep='first')
    df_IW37nBase = pd.merge(df_IW37nBase, Result_IW37nReporte, on='Orden-Semana',how='left')

    df_IW37nBase = pd.merge(df_IW37nBase, Result_Condiciones[['PtoTrbRes','Denominacion', 'AreaResponsable', 'Empresa', 'TipoContrato']], on='PtoTrbRes',how='left')
    df_IW37nBase = df_IW37nBase.drop_duplicates()
    df_IW37nBase['Status Sistema Reportado'] = df_IW37nBase['Status Sistema Reportado'].str[:9]
    Result_Condiciones.rename(columns={'StatusSistema':'Status Sistema Reportado'}, inplace=True)

    df_IW37nBase = pd.merge(df_IW37nBase, Result_Condiciones[['Status Sistema Reportado','StatusKPI']], on='Status Sistema Reportado',how='left')
    df_IW37nBase.rename(columns={'Orden_x':'Orden'}, inplace=True)
    df_IW37nBase["OTCerradas"] =  np.where(df_IW37nBase["StatusKPI"] == "Cerrado", df_IW37nBase["Orden"], 0) #StatusKPI si el valor es 0 es por que la OT esta abierta
    df_IW37nBase = df_IW37nBase.drop_duplicates()

    df_IW37nBase['Ubic.técn.'] = df_IW37nBase['Ubic.técn.'].fillna('-')
    df_IW37nBase['Seccion'] = df_IW37nBase['Ubic.técn.'].apply(lambda x: x.split('-')[1][:3])
    df_IW37nBase = df_IW37nBase[df_IW37nBase['Seccion'] != 'MI1']
    df_IW37nBase = df_IW37nBase.drop(columns=['Seccion'])
    df_IW37nBase["UT"] = df_IW37nBase["Ubic.técn."].str[:13].str.strip()
   
    #Columna concatenacion larga
    df_IW37nBase["Concatenacion"] = df_IW37nBase["Ubic.técn."].str.cat(df_IW37nBase["PtoTrbRes"], sep="-")
    df_IW37nBase["Concat"] = df_IW37nBase["UT"].str.cat(df_IW37nBase["PtoTrbRes"], sep="-")

    #Sacamos el área y subarea de la concatenación larga
    df_IW37nBase = pd.merge(df_IW37nBase, Result_Condiciones[['Concatenacion','Area', 'SubArea']], on='Concatenacion',how='left')
    df_IW37nBase = df_IW37nBase.drop_duplicates()
    df_IW37nBase['Vacío'] = df_IW37nBase.apply(lambda row: 'Sí' if pd.isnull(row['Area']) and pd.isnull(row['SubArea']) else 'No', axis=1)
 
    #merge con condicional
    df_vacios = df_IW37nBase[df_IW37nBase['Area'].isnull() | df_IW37nBase['SubArea'].isnull()] #Nuevo df con filas de area y subarea vacias
    result_unico = Result_Condiciones.drop_duplicates(subset='Concat', keep='first')
    df_actualizar = pd.merge(df_vacios,result_unico[['Concat', 'Area', 'SubArea']],on='Concat', how='left',suffixes=('', '_nuevo'))

    #Coloca el nuevo valor de la concatenacion en las columnas area y subarea
    df_actualizar['Area'] = df_actualizar.apply(lambda row: row['Area_nuevo'] if pd.isnull(row['Area']) else row['Area'], axis=1)
    df_actualizar['SubArea'] = df_actualizar.apply(lambda row: row['SubArea_nuevo'] if pd.isnull(row['SubArea']) else row['SubArea'], axis=1)
    df_actualizar = df_actualizar.drop(columns=['Area_nuevo', 'SubArea_nuevo'])
    df_IW37nBase = df_IW37nBase.dropna(subset=['Area', 'SubArea'])


    #Anido ambos data frames
    df_IW37nBase = pd.concat([df_IW37nBase, df_actualizar])
    df_IW37nBase = df_IW37nBase.drop_duplicates()

    df_IW37nBase.rename(columns={"Semana_x":"Semana"}, inplace=True)
    df_IW37nBase.rename(columns={'Trbjo real':'Trabajo Real'}, inplace=True)
    df_IW37nBase.rename(columns={'StatUsu':'Status Usuario'}, inplace=True)
    df_IW37nBase.rename(columns={'Revisión_y':'Revisión'}, inplace=True)


    return df_IW37nBase

@router.get("/GetAndProcessPlanMensual", tags=["Indicadores"])
async def Get_Process_PlanMensual ():

    df_result = []  
    df_PlanMensual = await Process_PlanMensual()

    function_return_Streaming(df_PlanMensual,df_result)
    def generate():
        for chunk in df_result:
            yield from chunk

    return StreamingResponse(generate(), media_type='application/json')


