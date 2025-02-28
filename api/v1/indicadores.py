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

load_dotenv()
router = APIRouter()


#Fechas
peru_tz = pytz.timezone("America/Lima")
current_date = datetime.datetime.now(peru_tz)
current_day = current_date.weekday()
current_week = current_date.isocalendar()[1]
current_month = current_date.month
current_year = current_date.year

last_date = current_date - datetime.timedelta(days=7)
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

@router.post("/asdasda",tags=["Indicadores"])
async def asdasd():
    RedisDockers.set('Process_Status_Data_SAP_Indicadores','completed')
    return ({"Message": "Oki Doki"})
    

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
        CursorIW29 = db.iw29.find({"Anho": "2025"})
        cursorIW37nBase = db.iw37n.find({"Anho": "2025"})
        CursorIW37nReporte = db.iw37nreport.find({"Anho": "2025"})
        CursorIW39 = db.iw39report.find({"Anho": "2025"})
        
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




#-----------------------------------------------------------------------

async def Process_IW39 ():    
    
    All_Data_IW39 = []
    df_result = []
    print("Fecha actual:", current_date)
    print("Zona horaria:", current_date.tzinfo)
    print("Semana: ",Semana,"Anho: ",Anho)
    print("Obteniendo datos de MongoDB IW39")
    CursorIW39 = db.iw39report.find({
        "Semana": str(Semana),
        "Anho": str(Anho)
    })
    
    print("Procesando los datos de MongoDB IW39")
    await id_to_string_process(CursorIW39,All_Data_IW39)
    df_IW39 = pd.DataFrame(All_Data_IW39)
        
    print("Creando el data frame IW39")
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

async def Process_IW37nReporte ():
    All_Data_IW37nReporte = []
    df_result = []  
    print("Obteniendo datos de MongoDB IW37nReporte")
    CursorIW37nReporte = db.iw37nreport.find({
        "Semana": str(Semana),
        "Anho": str(Anho)
    })
    
    print("Procesando los datos de MongoDB IW37nReporte")
    await id_to_string_process(CursorIW37nReporte,All_Data_IW37nReporte)
    df_IW37nReporte = pd.DataFrame(All_Data_IW37nReporte)
        
    print("Creando el data frame IW37nReporte")
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
        ]]
    return df_IW37nReporte

async def Process_IW37nBase ():
    All_Data_IW37nBase = []
    df_result = []  
    print("Obteniendo datos de MongoDB IW37nBase")
    CursorIW37nBase = db.iw37n.find({
        "Semana": str(Semana),
        "Anho": str(Anho)
    })
    
    print("Procesando los datos de MongoDB IW37nBase")
    await id_to_string_process(CursorIW37nBase,All_Data_IW37nBase)
    df_IW37nBase = pd.DataFrame(All_Data_IW37nBase)
        
    print("Creando los data frame IW37nBase")
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
    print("Obteniendo datos de MongoDB Condiciones")
    CursorIndicadores = db.baseindicadores.find({})
    
    print("Procesando los datos de MongoDB Condiciones")
    await id_to_string_process(CursorIndicadores,All_Data_Condiciones)
    df_Indicadores = pd.DataFrame(All_Data_Condiciones)
    
    return df_Indicadores

async def Process_IW37nBase_2 ():
     
    df_IW37nBase = await Process_IW37nBase()
    Result_IW37nReporte = await Process_IW37nReporte()
    Result_Condiciones = await Process_Condiciones()
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

async def Process_IW47 ():
    All_Data_IW47 = []
    print("Obteniendo datos de MongoDB IW47")
    # print("last_year: ", last_year)
    CursorIW47 = db.iw47.find({
        "Semana": str(Semana),
        "Anho": str(Anho)
    })
    
    print("Procesando los datos de MongoDB IW47")
    await id_to_string_process(CursorIW47,All_Data_IW47)
        
    print("Creando los data frame IW47")
    df_IW47 = pd.DataFrame(All_Data_IW47)
    df_IW37nBase = await Process_IW37nBase()
    df_IW37nReporte = await Process_IW37nReporte()
    df_Condiciones = await Process_Condiciones()
    df_IW39 = await Process_IW39()
    
    
    df_IW47 = df_IW47.drop_duplicates()
    df_IW47 = pd.merge(df_IW47,df_IW37nBase[['Orden','Revisión']], on='Orden',how='left')
    # df_IW47["RevisionIW47"] = "SEM" + df_IW47["Semana"].astype(str) + "-" + str(last_year)[-2:]
    df_IW47["RevisionIW47"] = "SEM" + df_IW47["Semana"].astype(str).str.zfill(2) + "-" + str(current_year)[-2:]
    df_IW47["Condicional"] = np.where(df_IW47["Revisión"] != df_IW47["RevisionIW47"], 1, 0)
    df_IW47 = df_IW47[df_IW47["Condicional"] == 1]
    
    df_IW47 = pd.merge(df_IW47,df_IW39[['Orden',"CpoClasif", "P", "PtoTrbRes", "Status del sistema", "StatUsu", "Texto breve", "Ubicación técnica"]], on='Orden',how='left')
    df_IW47.rename(columns={'P':'Prioridad'}, inplace=True)
    df_IW47.rename(columns={'Status del sistema':'Status Sistema Reportado'}, inplace=True)
    df_IW47["Status Sistema Reportado"] = df_IW47["Status Sistema Reportado"].str[:9].str.strip()
    df_IW47.drop(columns=["Condicional","Revisión"], inplace=True)
    df_IW47.rename(columns={'RevisionIW47':'Revisión'}, inplace=True)   
    df_IW47 = df_IW47.drop_duplicates()
    df_IW47 = pd.merge(df_IW47,df_IW37nReporte[['Orden','Inic.extr.']], on='Orden',how='left')
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
    
    return df_IW47

async def Process_IW29 ():
    All_Data_IW29 = []
    print("Obteniendo datos de MongoDB IW29")
    CursorIW29 = db.iw29.find({
        "Semana": str(Semana),
        "Anho": str(Anho)
    })
    
    print("Procesando los datos de MongoDB IW29")
    await id_to_string_process(CursorIW29,All_Data_IW29)
        
    print("Creando los data frame IW29")
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

#Proceso de Tratamiento de Datos
@router.get("/GetAndProcessIW39", tags=["Indicadores"])
async def Get_Process_IW39 ():
    df_result = []  
        
    df_IW39 = await Process_IW39()
    
    function_return_Streaming(df_IW39,df_result)
    def generate():
        for chunk in df_result:
            yield from chunk
    print("Finalizando el proceso de obtención y procesamiento de IW39 desde MongoDB")
    return StreamingResponse(generate(), media_type='application/json')


@router.get("/GetAndProcessIW37nReporte", tags=["Indicadores"])
async def Get_Process_IW37nReporte ():
    df_result = []  
    
    df_IW37nReporte = await Process_IW37nReporte()
    
    function_return_Streaming(df_IW37nReporte,df_result)
    def generate():
        for chunk in df_result:
            yield from chunk
    print("Finalizando el proceso de obtención y procesamiento de IW37nReporte desde MongoDB")
    return StreamingResponse(generate(), media_type='application/json')


@router.get("/GetAndProcessIW37nBase", tags=["Indicadores"])
async def Get_Process_IW37nReporte ():
    df_result = []  
    
    df_IW37nBase = await Process_IW37nBase_2()
    
    function_return_Streaming(df_IW37nBase,df_result)
    def generate():
        for chunk in df_result:
            yield from chunk
    print("Finalizando el proceso de obtención y procesamiento de IW37nBase desde MongoDB")
    return StreamingResponse(generate(), media_type='application/json')


@router.get("/GetAndProcessIW47", tags=["Indicadores"])
async def Get_Process_IW47 ():
    df_result = []  
    
    df_IW47 = await Process_IW47()
    
    function_return_Streaming(df_IW47,df_result)
    def generate():
        for chunk in df_result:
            yield from chunk
    print("Finalizando el proceso de obtención y procesamiento de IW47 desde MongoDB")
    return StreamingResponse(generate(), media_type='application/json')

@router.get("/GetAndProcessIW29", tags=["Indicadores"])
async def Get_Process_IW29 ():
    df_result = []  
    
    df_IW29 = await Process_IW29()
    
    function_return_Streaming(df_IW29,df_result)
    def generate():
        for chunk in df_result:
            yield from chunk
    print("Finalizando el proceso de obtención y procesamiento de IW29 desde MongoDB")
    return StreamingResponse(generate(), media_type='application/json')







@router.get("/Pruebafechas", tags=["Indicadores"])
async def Prueba_fechas ():
    return{
        "current_date":current_date,
        "current_week":current_week,
        "last_week":last_week,
        "Semana":Semana,
        "Anho":Anho
    }
    