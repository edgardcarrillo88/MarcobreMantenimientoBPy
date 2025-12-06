from fastapi import APIRouter, Query
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
from typing import List, Optional


load_dotenv()
router = APIRouter()

#Funciones

async def id_to_string_process(cursor, array):
    
    async for item in cursor:
        item['_id'] = str(item['_id']) 
        array.append(item)
    return array

def function_return_Streaming(dataframes, result_list):
    def generate():
        buffer = io.StringIO()
        buffer.write('{')  # Inicia JSON
        first_outer = True

        for outer_key, outer_value in dataframes.items():
            if not first_outer:
                buffer.write(',')
            first_outer = False

            buffer.write(f'"{outer_key}":')
            buffer.write('{')
            first_inner = True

            for inner_key, df in outer_value.items():
                if not isinstance(df, pd.DataFrame):
                    continue  # O podrías incluirlo como valor escalar

                if not first_inner:
                    buffer.write(',')
                first_inner = False

                buffer.write(f'"{inner_key}":[')
                first_chunk = True

                chunk_size = 1000
                for i in range(0, len(df), chunk_size):
                    chunk = df.iloc[i:i + chunk_size]
                    if not first_chunk:
                        buffer.write(',')
                    first_chunk = False

                    chunk_json = chunk.to_json(orient='records')
                    buffer.write(chunk_json[1:-1])  # Sin corchetes externos
                    yield buffer.getvalue()
                    buffer.truncate(0)
                    buffer.seek(0)

                buffer.write(']')

            buffer.write('}')  # Cierra objeto interior (ej. "CurvaGeneral")
        
        buffer.write('}')  # Cierra objeto global
        yield buffer.getvalue()
    
    result_list.append(generate())



REDISHOST = os.getenv("REDISHOST")
REDISPORT = os.getenv("REDISPORT")
REDISUSER = os.getenv("REDISUSER")
REDISPASSWORD = os.getenv("REDISPASSWORD")


#Conectandose al servidor de redis, que entiendo esta en mi conteder de dockers
RedisDockers = redis.Redis(host=REDISHOST, port=REDISPORT,username=REDISUSER,password=REDISPASSWORD, db=0)


#PENDIENTE REVISAR LA GENERACIÓN DE LAS CURVAS REALES AJUSTADAS!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
async def Linea_Base():
    All_Data_LineaBase = []
    print("Obteniendo datos de MongoDB LineaBase")
    CursorActivities = db.Activities.find({})
    
    print("Procesando los datos de MongoDB LIneaBase")
    await id_to_string_process(CursorActivities,All_Data_LineaBase)
    df_LineaBase = pd.DataFrame(All_Data_LineaBase)
    df_Real = pd.DataFrame(All_Data_LineaBase)
    

    print("Creando el data frame de Línea base")
    df_LineaBase.sort_values(by=['id'], inplace=True)
    df_LineaBase['inicioplan'] = df_LineaBase['inicioplan'].apply(lambda x: x.replace(microsecond=100000))
    df_LineaBase['finplan'] = df_LineaBase['finplan'].apply(lambda x: x.replace(microsecond=100000))
    df_LineaBase["DifHorasTime"] = (df_LineaBase["finplan"] - df_LineaBase["inicioplan"])
    df_LineaBase["DifHorasHorasNoRounded"] = ((df_LineaBase["finplan"] - df_LineaBase["inicioplan"]).dt.total_seconds() / 3600)
    df_LineaBase["DifHorasHoras"] = ((df_LineaBase["finplan"] - df_LineaBase["inicioplan"]).dt.total_seconds() / 3600).apply(math.ceil)
    df_LineaBase["hh"] = df_LineaBase["DifHorasHorasNoRounded"] / df_LineaBase["DifHorasHoras"]


    df_LineaBase['Ejex'] = df_LineaBase.apply(lambda row: [row['inicioplan'] + timedelta(hours=i) for i in range(row['DifHorasHoras'] )], axis=1)

    df_LineaBase = df_LineaBase.explode('Ejex')


    df_LineaBase['Ejex'] = df_LineaBase['Ejex'].dt.ceil('h')
    df_LineaBase.rename(columns={"hh":"hh_lb"}, inplace=True)
 

    df_LineaBase_Ajustada = df_LineaBase[df_LineaBase['ActividadCancelada']=="No"]
    df_Real_Ajustada = df_Real[df_Real['ActividadCancelada']=="No"]
    
    return {"df_LineaBase":df_LineaBase, "df_Real":df_Real, "df_LineaBase_Ajustada": df_LineaBase_Ajustada, "df_Real_Ajustada": df_Real_Ajustada}

def Linea_Avance_General(df_LineaBase, df_Real, df_LineaBase_Ajustada, df_Real_Ajustada):
    
    df_LineaBase_0 = df_LineaBase.groupby('Ejex')['hh_lb'].sum().reset_index()
    df_LineaBase_0["hh_lb_cum"] = df_LineaBase_0["hh_lb"].cumsum()


    
    df_LineaBase_Ajustada_0 = df_LineaBase_Ajustada.groupby('Ejex')['hh_lb'].sum().reset_index()
    #df_LineaBase_Ajustada_0["hh_lb_cum"] = df_LineaBase_Ajustada_0["hh_lb"].cumsum() ####################################
    df_LineaBase_Ajustada_0.rename(columns={'hh':'hh_lb'}, inplace=True)
    
    df_Real_0 = df_Real.groupby('Ejex')['hh'].sum().reset_index()
    #df_Real_0["hh_real_cum"] = df_Real_0["hh"].cumsum() ############################################
    df_Real_0.rename(columns={'hh':'hh_real'}, inplace=True)
    
    df_Real_Ajustada_0 = df_Real.groupby('Ejex')['hh'].sum().reset_index()
    df_Real_Ajustada_0["hh_real_cum"] = df_Real_0["hh_real"].cumsum()
    #df_Real_Ajustada_0.rename(columns={'hh':'hh_real'}, inplace=True) 
    
    result = Rango_Eje_X(df_LineaBase_0, df_Real_0, df_LineaBase_Ajustada_0, df_Real_Ajustada_0)



    
    
    #Uniendos los dataframes de linea base y linea real y renombrando las columnas
    df_LineaGeneral = result["df_ejeX_Normal"].merge(df_LineaBase_0, on="Ejex", how="left").merge(df_Real_0, on="Ejex", how="left")
    df_LineaGeneral.fillna({"hh_lb": 0, "hh_real": 0}, inplace=True)

    df_LineaGeneral["hh_lb_cum"] = df_LineaGeneral["hh_lb"].cumsum() ############################################
    df_LineaGeneral["hh_real_cum"] = df_LineaGeneral["hh_real"].cumsum() ############################################
    #df_LineaGeneral.fillna({"hh_lb": 0, "hh_real": 0, "hh_lb_cum": 0, "hh_real_cum": 0}, inplace=True)

    
    #Uniendos los dataframes de linea base ajustada y linea real y renombrando las columnas
    df_LineaGeneral_Ajustada = result["df_ejeX_Ajustada"].merge(df_LineaBase_Ajustada_0, on="Ejex", how="left").merge(df_Real_0, on="Ejex", how="left")
    df_LineaGeneral_Ajustada.fillna({"hh_lb": 0, "hh_real": 0}, inplace=True)

    df_LineaGeneral_Ajustada["hh_lb_cum"] = df_LineaGeneral_Ajustada["hh_lb"].cumsum() ############################################
    df_LineaGeneral_Ajustada["hh_real_cum"] = df_LineaGeneral_Ajustada["hh_real"].cumsum() ############################################
    #df_LineaGeneral_Ajustada.fillna({"hh_lb": 0, "hh_real": 0, "hh_lb_cum": 0, "hh_real_cum": 0}, inplace=True)

    df_Real_0["hh_real_cum"] = df_Real_0["hh_real"].cumsum()
    df_LineaBase_Ajustada_0["hh_lb_cum"] = df_LineaBase_Ajustada_0["hh_lb"].cumsum()


    Avances = Calculo_Totales(df_LineaBase_0,df_Real_0,df_LineaBase_Ajustada_0,df_Real_Ajustada_0)
    
    return{
        "df_LineaGeneral": df_LineaGeneral,
        "df_LineaGeneral_Ajustada": df_LineaGeneral_Ajustada,
        "AvanceGeneral": Avances["AvanceReal"],
        "AvanceGeneralAjustado": Avances["AvanceAjustado"]
    }

def Rango_Eje_X (df_LineaBase_0, df_Real_0, df_LineaBase_Ajustada_0, df_Real_Ajustada_0):
    #Esto es para la curva General
    #------------------------------------------------------------------------------------------------------------
    #Aca determino la fecha mas temprana y mas tardia entre la linea base y la linea real
    start_date = min(df_LineaBase_0["Ejex"].min(), df_Real_0["Ejex"].min())
    end_date = max(df_LineaBase_0["Ejex"].max(), df_Real_0["Ejex"].max())
    #end_date = pd.to_datetime('2024-12-15')  #|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
    
    #Aca determino la fecha mas temprana y mas tardia entre la linea base ajustada y la linea real
    start_date2 = min(df_LineaBase_Ajustada_0["Ejex"].min(), df_Real_Ajustada_0["Ejex"].min())
    end_date2 = max(df_LineaBase_Ajustada_0["Ejex"].max(), df_Real_Ajustada_0["Ejex"].max())
    #end_date2 = pd.to_datetime('2024-12-15')  #|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
    
    #Aca creo un rango de fechas con la fecha mas temprana y mas tardia de la curva regular con saltos de una hora
    ejeXnew = pd.date_range(start=start_date, end=end_date, freq="1h")
    df_ejeXnew_Normal  = pd.DataFrame({"Ejex": ejeXnew})
    
    #Aca creo un rango de fechas con la fecha mas temprana y mas tardia de la curva ajustada con saltos de una hora
    ejeXnew2 = pd.date_range(start=start_date2, end=end_date2, freq="1h")
    df_ejeXnew_Ajustada  = pd.DataFrame({"Ejex": ejeXnew2})
    #-------------------------------------------------------------------------------------------------------------
    return {
        "df_ejeX_Normal":df_ejeXnew_Normal,
        "df_ejeX_Ajustada":df_ejeXnew_Ajustada
    }

def Linea_Avance_Area(df_LineaBase, df_Real, df_LineaBase_Ajustada, df_Real_Ajustada):
    print("Creando el dataframe de curva base area ajustada")
    df_LineaBase_Area = df_LineaBase.groupby(["Ejex","area"])["hh_lb"].sum().reset_index()
    df_LineaBase_Area.sort_values(["area", "Ejex"], inplace=True)
    #df_LineaBase_Area["hh_lb_cum"] = df_LineaBase_Area.groupby("area")["hh_lb"].cumsum()
    df_LineaBase_Area.rename(columns={"area":"Filtro01"}, inplace=True)
    
    df_LineaBase_Area_Ajustada = df_LineaBase_Ajustada.groupby(["Ejex","area"])["hh_lb"].sum().reset_index()
    df_LineaBase_Area_Ajustada.sort_values(["area", "Ejex"], inplace=True)
    #df_LineaBase_Area_Ajustada["hh_lb_cum"] = df_LineaBase_Area_Ajustada.groupby("area")["hh_lb"].cumsum()
    df_LineaBase_Area_Ajustada.rename(columns={"area":"Filtro01"}, inplace=True)
    
    df_Real_Area = df_Real.groupby(["Ejex","area"])["hh"].sum().reset_index()
    df_Real_Area.sort_values(["area", "Ejex"], inplace=True)
    #df_Real_Area["hh_real_cum"] = df_Real_Area.groupby("area")["hh"].cumsum()
    df_Real_Area.rename(columns={"hh":"hh_real"}, inplace=True)
    df_Real_Area.rename(columns={"area":"Filtro01"}, inplace=True)
    
    df_Real_Area_Ajustada = df_Real_Ajustada.groupby(["Ejex","area"])["hh"].sum().reset_index()
    df_Real_Area_Ajustada.sort_values(["area", "Ejex"], inplace=True)
    #df_Real_Area_Ajustada["hh_real_cum"] = df_Real_Area_Ajustada.groupby("area")["hh"].cumsum()
    df_Real_Area_Ajustada.rename(columns={"hh":"hh_real"}, inplace=True)
    df_Real_Area_Ajustada.rename(columns={"area":"Filtro01"}, inplace=True)
    
    result = Rango_Eje_X_Area(df_LineaBase_Area, df_Real_Area, df_LineaBase_Area_Ajustada, df_Real_Area_Ajustada)
    
        
    df_LineaArea_Total = (result["df_ejeX_Normal"].merge(df_LineaBase_Area, on=["Ejex", "Filtro01"], how="left").merge(df_Real_Area, on=["Ejex", "Filtro01"], how="left"))
    df_LineaArea_Total.fillna(0, inplace=True)
    
        
    df_LineaArea_Total["hh_lb_cum"] = (
    df_LineaArea_Total
    .groupby("Filtro01")["hh_lb"]
    .cumsum()
    )

    df_LineaArea_Total["hh_real_cum"] = (
        df_LineaArea_Total
        .groupby("Filtro01")["hh_real"]
        .cumsum()
    )
    
    df_LineaArea_Ajustada = (result["df_ejeX_Ajustada"].merge(df_LineaBase_Area_Ajustada, on=["Ejex", "Filtro01"], how="left").merge(df_Real_Area_Ajustada, on=["Ejex", "Filtro01"], how="left"))
    df_LineaArea_Ajustada.fillna(0, inplace=True)
    
    df_LineaArea_Ajustada["hh_lb_cum"] = (
    df_LineaArea_Ajustada
    .groupby("Filtro01")["hh_lb"]
    .cumsum()
    )

    df_LineaArea_Ajustada["hh_real_cum"] = (
        df_LineaArea_Ajustada
        .groupby("Filtro01")["hh_real"]
        .cumsum()
    )
    
    return {
        "df_LineaArea_Total": df_LineaArea_Total,
        "df_LineaArea_Ajustada": df_LineaArea_Ajustada
    }

def Rango_Eje_X_Area (df_LineaBase_Area, df_Real_Area, df_LineaBase_Area_Ajustada, df_Real_Area_Ajustada):
    start_date = min(df_LineaBase_Area["Ejex"].min(), df_Real_Area["Ejex"].min())
    end_date = max(df_LineaBase_Area["Ejex"].max(), df_Real_Area["Ejex"].max())
    #end_date = pd.to_datetime('2024-12-15')  #|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
    
    #Aca determino la fecha mas temprana y mas tardia entre la linea base ajustada y la linea real
    start_date2 = min(df_LineaBase_Area_Ajustada["Ejex"].min(), df_Real_Area["Ejex"].min())
    end_date2 = max(df_LineaBase_Area_Ajustada["Ejex"].max(), df_Real_Area["Ejex"].max())
    #end_date2 = pd.to_datetime('2024-12-15')  #|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
    
    #Aca creo un rango de fechas con la fecha mas temprana y mas tardia de la curva regular con saltos de una hora
    ejeXNormal = pd.date_range(start=start_date, end=end_date, freq="1h")
    #df_ejeXnew  = pd.DataFrame({"Ejex": ejeXnew})
    
    
    #Aca creo un rango de fechas con la fecha mas temprana y mas tardia de la curva ajustada con saltos de una hora
    ejeXnewAjustada = pd.date_range(start=start_date2, end=end_date2, freq="1h")
    #df_ejeXnew2  = pd.DataFrame({"Ejex": ejeXnew2})
    
    areas = df_LineaBase_Area["Filtro01"].unique()
    
    df_ejeX_Normal = pd.MultiIndex.from_product([ejeXNormal, areas], names=["Ejex", "Filtro01"]).to_frame(index=False)
    df_ejeX_Ajustada = pd.MultiIndex.from_product([ejeXnewAjustada, areas], names=["Ejex", "Filtro01"]).to_frame(index=False)
    
    return {
        "df_ejeX_Normal":df_ejeX_Normal,
        "df_ejeX_Ajustada":df_ejeX_Ajustada
    }

def Linea_Avance_Contratista(df_LineaBase, df_Real, df_LineaBase_Ajustada, df_Real_Ajustada):

    # print("CBT")
    # print(df_Real[df_Real["contratista"]=="CBT"])

    df_LineaBase_Contratista = df_LineaBase.groupby(["Ejex","contratista"])["hh_lb"].sum().reset_index()


    df_LineaBase_Contratista.sort_values(["contratista", "Ejex"], inplace=True)
    df_LineaBase_Contratista.rename(columns={"contratista":"Filtro01"}, inplace=True)
    
    df_LineaBase_Contratista_Ajustada = df_LineaBase_Ajustada.groupby(["Ejex","contratista"])["hh_lb"].sum().reset_index()
    df_LineaBase_Contratista_Ajustada.sort_values(["contratista", "Ejex"], inplace=True)
    df_LineaBase_Contratista_Ajustada.rename(columns={"contratista":"Filtro01"}, inplace=True)
    
    df_Real_Contratista = df_Real.groupby(["Ejex","contratista"])["hh"].sum().reset_index() # Creo que debeería cambiar el HH por el EV
    #df_Real_Contratista = df_Real.groupby(["Ejex","contratista"])["EV"].sum().reset_index() # Creo que debeería cambiar el HH por el EV

    
    df_Real_Contratista.sort_values(["contratista", "Ejex"], inplace=True)
    df_Real_Contratista.rename(columns={"hh":"hh_real"}, inplace=True)
    #df_Real_Contratista.rename(columns={"EV":"hh_real"}, inplace=True)
    df_Real_Contratista.rename(columns={"contratista":"Filtro01"}, inplace=True)

    
    df_Real_Contratista_Ajustada = df_Real_Ajustada.groupby(["Ejex","contratista"])["hh"].sum().reset_index()
    df_Real_Contratista_Ajustada.sort_values(["contratista", "Ejex"], inplace=True)
    df_Real_Contratista_Ajustada.rename(columns={"hh":"hh_real"}, inplace=True)
    df_Real_Contratista_Ajustada.rename(columns={"contratista":"Filtro01"}, inplace=True)
    
    result = Rango_Eje_X_Contratista(df_LineaBase_Contratista, df_Real_Contratista, df_LineaBase_Contratista_Ajustada, df_Real_Contratista_Ajustada)
    
    df_LineaContratista_Total = (result["df_ejeX_Normal"].merge(df_LineaBase_Contratista, on=["Ejex", "Filtro01"], how="left").merge(df_Real_Contratista, on=["Ejex", "Filtro01"], how="left"))
    df_LineaContratista_Total.fillna(0, inplace=True)

    
    df_LineaContratista_Total["hh_lb_cum"] = (
    df_LineaContratista_Total
    .groupby("Filtro01")["hh_lb"]
    .cumsum()
    )

    df_LineaContratista_Total["hh_real_cum"] = (
        df_LineaContratista_Total
        .groupby("Filtro01")["hh_real"]
        .cumsum()
    )

 
        
    df_LineaContratista_Ajustada = (result["df_ejeX_Ajustada"].merge(df_LineaBase_Contratista_Ajustada, on=["Ejex", "Filtro01"], how="left").merge(df_Real_Contratista_Ajustada, on=["Ejex", "Filtro01"], how="left"))
    df_LineaContratista_Ajustada.fillna(0, inplace=True)
    
    df_LineaContratista_Ajustada["hh_lb_cum"] = (
    df_LineaContratista_Ajustada
    .groupby("Filtro01")["hh_lb"]
    .cumsum()
    )

    df_LineaContratista_Ajustada["hh_real_cum"] = (
        df_LineaContratista_Ajustada
        .groupby("Filtro01")["hh_real"]
        .cumsum()
    )
    
    return {
        "df_LineaContratista_Total": df_LineaContratista_Total,
        "df_LineaContratista_Ajustada": df_LineaContratista_Ajustada
    }

def Rango_Eje_X_Contratista (df_LineaBase_Contratista, df_Real_Contratista, df_LineaBase_Contratista_Ajustada, df_Real_Contratista_Ajustada):
    
    #Aca determino la fecha mas temprana y mas tardia entre la linea base REGULAR y la linea real
    start_date = min(df_LineaBase_Contratista["Ejex"].min(), df_Real_Contratista["Ejex"].min())
    end_date = max(df_LineaBase_Contratista["Ejex"].max(), df_Real_Contratista["Ejex"].max())
    #end_date = pd.to_datetime('2024-12-15')  #|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
    
    #Aca determino la fecha mas temprana y mas tardia entre la linea base AJUSTADA y la linea real
    start_date2 = min(df_LineaBase_Contratista_Ajustada["Ejex"].min(), df_Real_Contratista["Ejex"].min())
    end_date2 = max(df_LineaBase_Contratista_Ajustada["Ejex"].max(), df_Real_Contratista["Ejex"].max())
    #end_date2 = pd.to_datetime('2024-12-15')  #|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
    
    #Aca creo un rango de fechas con la fecha mas temprana y mas tardia de la curva regular con saltos de una hora
    ejeXNormal = pd.date_range(start=start_date, end=end_date, freq="1h")
    
    
    #Aca creo un rango de fechas con la fecha mas temprana y mas tardia de la curva ajustada con saltos de una hora
    ejeXnewAjustada = pd.date_range(start=start_date2, end=end_date2, freq="1h")
    
    contratistas = df_LineaBase_Contratista["Filtro01"].unique()
    
    df_ejeX_Normal = pd.MultiIndex.from_product([ejeXNormal, contratistas], names=["Ejex", "Filtro01"]).to_frame(index=False)
    df_ejeX_Ajustada = pd.MultiIndex.from_product([ejeXnewAjustada, contratistas], names=["Ejex", "Filtro01"]).to_frame(index=False)

    
    return {
        "df_ejeX_Normal":df_ejeX_Normal,
        "df_ejeX_Ajustada":df_ejeX_Ajustada
    }

def Linea_Avance_Area_Contratista(df_LineaBase, df_Real, df_LineaBase_Ajustada, df_Real_Ajustada):


    df_LineaBase_Area_Contratista = df_LineaBase.groupby(["Ejex","contratista","area"])["hh_lb"].sum().reset_index()
    df_LineaBase_Area_Contratista.sort_values(["contratista","area", "Ejex"], inplace=True)
    #df_LineaBase_Contratista["hh_lb_cum"] = df_LineaBase_Contratista.groupby("contratista")["hh_lb"].cumsum()
    df_LineaBase_Area_Contratista.rename(columns={"contratista":"Filtro01", "area":"Filtro02"}, inplace=True)
    
    df_LineaBase_Area_Contratista_Ajustada = df_LineaBase_Ajustada.groupby(["Ejex","contratista","area"])["hh_lb"].sum().reset_index()
    df_LineaBase_Area_Contratista_Ajustada.sort_values(["contratista","area", "Ejex"], inplace=True)
    #df_LineaBase_Contratista_Ajustada["hh_lb_cum"] = df_LineaBase_Contratista_Ajustada.groupby("contratista")["hh_lb"].cumsum()
    df_LineaBase_Area_Contratista_Ajustada.rename(columns={"contratista":"Filtro01", "area":"Filtro02"}, inplace=True)
    
    df_Real_Area_Contratista = df_Real.groupby(["Ejex","contratista","area"])["hh"].sum().reset_index()

    

    df_Real_Area_Contratista.sort_values(["contratista","area", "Ejex"], inplace=True)
    #df_Real_Contratista["hh_real_cum"] = df_Real_Contratista.groupby("contratista")["hh"].cumsum()
    df_Real_Area_Contratista.rename(columns={"hh":"hh_real"}, inplace=True)
    df_Real_Area_Contratista.rename(columns={"contratista":"Filtro01", "area":"Filtro02"}, inplace=True)


    
    
    df_Real_Area_Contratista_Ajustada = df_Real_Ajustada.groupby(["Ejex","contratista","area"])["hh"].sum().reset_index()
    df_Real_Area_Contratista_Ajustada.sort_values(["contratista","area", "Ejex"], inplace=True)
    #df_Real_Contratista_Ajustada["hh_real_cum"] = df_Real_Contratista_Ajustada.groupby("contratista")["hh"].cumsum()
    df_Real_Area_Contratista_Ajustada.rename(columns={"hh":"hh_real"}, inplace=True)
    df_Real_Area_Contratista_Ajustada.rename(columns={"contratista":"Filtro01", "area":"Filtro02"}, inplace=True)
    
    result = Rango_Eje_X_Area_Contratista(df_LineaBase_Area_Contratista, df_Real_Area_Contratista, df_LineaBase_Area_Contratista_Ajustada, df_Real_Area_Contratista_Ajustada)
    
  
    df_LineaAreaContratista_Total = (result["df_ejeX_Normal"].merge(df_LineaBase_Area_Contratista, on=["Ejex", "Filtro01", "Filtro02"], how="left").merge(df_Real_Area_Contratista, on=["Ejex", "Filtro01", "Filtro02"], how="left"))
    df_LineaAreaContratista_Total.fillna(0, inplace=True)




    df_LineaAreaContratista_Total["hh_lb_cum"] = (df_LineaAreaContratista_Total.groupby(["Filtro01", "Filtro02"])["hh_lb"].cumsum())
    df_LineaAreaContratista_Total["hh_real_cum"] = (df_LineaAreaContratista_Total.groupby(["Filtro01", "Filtro02"])["hh_real"].cumsum())


        
    df_LineaAreaContratista_Ajustada = (result["df_ejeX_Ajustada"].merge(df_LineaBase_Area_Contratista_Ajustada, on=["Ejex", "Filtro01", "Filtro02"], how="left").merge(df_Real_Area_Contratista_Ajustada, on=["Ejex", "Filtro01", "Filtro02"], how="left"))
    df_LineaAreaContratista_Ajustada.fillna(0, inplace=True)
    df_LineaAreaContratista_Ajustada["hh_lb_cum"] = (df_LineaAreaContratista_Ajustada.groupby(["Filtro01", "Filtro02"])["hh_lb"].cumsum())


    df_LineaAreaContratista_Ajustada["hh_real_cum"] = (df_LineaAreaContratista_Ajustada.groupby(["Filtro01", "Filtro02"])["hh_real"].cumsum())
    
    return {
        "df_LineaAreaContratista_Total": df_LineaAreaContratista_Total,
        "df_LineaAreaContratista_Ajustada": df_LineaAreaContratista_Ajustada
    }

def Rango_Eje_X_Area_Contratista (df_LineaBase_Area_Contratista, df_Real_Area_Contratista, df_LineaBase_Area_Contratista_Ajustada, df_Real_Area_Contratista_Ajustada):
    start_date = min(df_LineaBase_Area_Contratista["Ejex"].min(), df_Real_Area_Contratista["Ejex"].min())
    end_date = max(df_LineaBase_Area_Contratista["Ejex"].max(), df_Real_Area_Contratista["Ejex"].max())
    #end_date = pd.to_datetime('2024-12-15')  #|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
    
    #Aca determino la fecha mas temprana y mas tardia entre la linea base ajustada y la linea real
    start_date2 = min(df_LineaBase_Area_Contratista_Ajustada["Ejex"].min(), df_Real_Area_Contratista["Ejex"].min())
    end_date2 = max(df_LineaBase_Area_Contratista_Ajustada["Ejex"].max(), df_Real_Area_Contratista["Ejex"].max())
    #end_date2 = pd.to_datetime('2024-12-15')  #|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
    
    #Aca creo un rango de fechas con la fecha mas temprana y mas tardia de la curva regular con saltos de una hora
    ejeXNormal = pd.date_range(start=start_date, end=end_date, freq="1h")
    #df_ejeXnew  = pd.DataFrame({"Ejex": ejeXnew})
    
    
    #Aca creo un rango de fechas con la fecha mas temprana y mas tardia de la curva ajustada con saltos de una hora
    ejeXnewAjustada = pd.date_range(start=start_date2, end=end_date2, freq="1h")
    #df_ejeXnew2  = pd.DataFrame({"Ejex": ejeXnew2})
    
    contratistas = df_LineaBase_Area_Contratista["Filtro01"].unique()
    areas = df_LineaBase_Area_Contratista["Filtro02"].unique()
    
    df_ejeX_Normal = pd.MultiIndex.from_product([ejeXNormal, contratistas, areas], names=["Ejex", "Filtro01", "Filtro02"]).to_frame(index=False)
    df_ejeX_Ajustada = pd.MultiIndex.from_product([ejeXnewAjustada, contratistas, areas], names=["Ejex", "Filtro01", "Filtro02"]).to_frame(index=False)
    
    return {
        "df_ejeX_Normal":df_ejeX_Normal,
        "df_ejeX_Ajustada":df_ejeX_Ajustada
    }

def Linea_Avance_Contratista_Especialidad(df_LineaBase, df_Real, df_LineaBase_Ajustada, df_Real_Ajustada):
    df_LineaBase_Especialidad_Contratista = df_LineaBase.groupby(["Ejex","contratista","especialidad"])["hh_lb"].sum().reset_index()
    df_LineaBase_Especialidad_Contratista.sort_values(["contratista","especialidad", "Ejex"], inplace=True)
    df_LineaBase_Especialidad_Contratista.rename(columns={"contratista":"Filtro01", "especialidad":"Filtro02"}, inplace=True)
    
    df_LineaBase_Especialidad_Contratista_Ajustada = df_LineaBase_Ajustada.groupby(["Ejex","contratista","especialidad"])["hh_lb"].sum().reset_index()
    df_LineaBase_Especialidad_Contratista_Ajustada.sort_values(["contratista","especialidad", "Ejex"], inplace=True)
    df_LineaBase_Especialidad_Contratista_Ajustada.rename(columns={"contratista":"Filtro01", "especialidad":"Filtro02"}, inplace=True)
    
    df_Real_Especialidad_Contratista = df_Real.groupby(["Ejex","contratista","especialidad"])["hh"].sum().reset_index()
    df_Real_Especialidad_Contratista.sort_values(["contratista","especialidad", "Ejex"], inplace=True)
    df_Real_Especialidad_Contratista.rename(columns={"hh":"hh_real"}, inplace=True)
    df_Real_Especialidad_Contratista.rename(columns={"contratista":"Filtro01", "especialidad":"Filtro02"}, inplace=True)
    
    df_Real_Especialidad_Contratista_Ajustada = df_Real_Ajustada.groupby(["Ejex","contratista","especialidad"])["hh"].sum().reset_index()
    df_Real_Especialidad_Contratista_Ajustada.sort_values(["contratista","especialidad", "Ejex"], inplace=True)
    df_Real_Especialidad_Contratista_Ajustada.rename(columns={"hh":"hh_real"}, inplace=True)
    df_Real_Especialidad_Contratista_Ajustada.rename(columns={"contratista":"Filtro01", "especialidad":"Filtro02"}, inplace=True)
    
    result = Rango_Eje_X_Area_Contratista(df_LineaBase_Especialidad_Contratista, df_Real_Especialidad_Contratista, df_LineaBase_Especialidad_Contratista_Ajustada, df_Real_Especialidad_Contratista_Ajustada)
    
  
    df_LineaEspecialidadContratista_Total = (result["df_ejeX_Normal"].merge(df_LineaBase_Especialidad_Contratista, on=["Ejex", "Filtro01", "Filtro02"], how="left").merge(df_Real_Especialidad_Contratista, on=["Ejex", "Filtro01", "Filtro02"], how="left"))
    df_LineaEspecialidadContratista_Total.fillna(0, inplace=True)
    df_LineaEspecialidadContratista_Total["hh_lb_cum"] = (df_LineaEspecialidadContratista_Total.groupby(["Filtro01", "Filtro02"])["hh_lb"].cumsum())
    df_LineaEspecialidadContratista_Total["hh_real_cum"] = (df_LineaEspecialidadContratista_Total.groupby(["Filtro01", "Filtro02"])["hh_real"].cumsum())
    
        
    df_LineaEspecialidadContratista_Ajustada = (result["df_ejeX_Ajustada"].merge(df_LineaBase_Especialidad_Contratista_Ajustada, on=["Ejex", "Filtro01", "Filtro02"], how="left").merge(df_Real_Especialidad_Contratista_Ajustada, on=["Ejex", "Filtro01", "Filtro02"], how="left"))
    df_LineaEspecialidadContratista_Ajustada.fillna(0, inplace=True)
    df_LineaEspecialidadContratista_Ajustada["hh_lb_cum"] = (df_LineaEspecialidadContratista_Ajustada.groupby(["Filtro01", "Filtro02"])["hh_lb"].cumsum())
    df_LineaEspecialidadContratista_Ajustada["hh_real_cum"] = (df_LineaEspecialidadContratista_Ajustada.groupby(["Filtro01", "Filtro02"])["hh_real"].cumsum())
    
    return {
        "df_LineaEspecialidadContratista_Total": df_LineaEspecialidadContratista_Total,
        "df_LineaEspecialidadContratista_Ajustada": df_LineaEspecialidadContratista_Ajustada
    }

def Linea_Avance_WhatIf(df_LineaBase, df_Real, df_LineaBase_Ajustada, df_Real_Ajustada):
    
    df_LineaBase = df_LineaBase.copy()
    df_Real = df_Real.copy()
    df_LineaBase_Ajustada = df_LineaBase_Ajustada.copy()
    df_Real_Ajustada = df_Real_Ajustada.copy()
    
    df_LineaBase["Contratista-Especialidad"] = df_LineaBase["contratista"] + "-" + df_LineaBase["especialidad"]
    df_Real["Contratista-Especialidad"] = df_Real["contratista"] + "-" + df_Real["especialidad"]
    df_LineaBase_Ajustada["Contratista-Especialidad"] = df_LineaBase_Ajustada["contratista"] + "-" + df_LineaBase_Ajustada["especialidad"]
    df_Real_Ajustada["Contratista-Especialidad"] = df_Real_Ajustada["contratista"] + "-" + df_Real_Ajustada["especialidad"]
    
    df_LineaBase_WhatIf = df_LineaBase.groupby(["Ejex","Contratista-Especialidad","area"])["hh_lb"].sum().reset_index()
    df_LineaBase_WhatIf.sort_values(["Contratista-Especialidad","area", "Ejex"], inplace=True)
    df_LineaBase_WhatIf.rename(columns={"Contratista-Especialidad":"Filtro01", "area":"Filtro02"}, inplace=True)
    
    df_LineaBase_Ajustada_WhatIf = df_LineaBase_Ajustada.groupby(["Ejex","Contratista-Especialidad","area"])["hh_lb"].sum().reset_index()
    df_LineaBase_Ajustada_WhatIf.sort_values(["area","Contratista-Especialidad", "Ejex"], inplace=True)
    df_LineaBase_Ajustada_WhatIf.rename(columns={"Contratista-Especialidad":"Filtro01", "area":"Filtro02"}, inplace=True)
    
    df_Real_WhatIf = df_Real.groupby(["Ejex","Contratista-Especialidad","area"])["hh"].sum().reset_index()
    df_Real_WhatIf.sort_values(["Contratista-Especialidad","area", "Ejex"], inplace=True)
    df_Real_WhatIf.rename(columns={"hh":"hh_real"}, inplace=True)
    df_Real_WhatIf.rename(columns={"Contratista-Especialidad":"Filtro01", "area":"Filtro02"}, inplace=True)
    
    df_Real_Ajustada_WhatIf = df_Real_Ajustada.groupby(["Ejex","Contratista-Especialidad","area"])["hh"].sum().reset_index()
    df_Real_Ajustada_WhatIf.sort_values(["Contratista-Especialidad","area", "Ejex"], inplace=True)
    df_Real_Ajustada_WhatIf.rename(columns={"hh":"hh_real"}, inplace=True)
    df_Real_Ajustada_WhatIf.rename(columns={"Contratista-Especialidad":"Filtro01", "area":"Filtro02"}, inplace=True)
    
    result = Rango_Eje_X_Area_Contratista(df_LineaBase_WhatIf, df_Real_WhatIf, df_LineaBase_Ajustada_WhatIf, df_Real_Ajustada_WhatIf)
    
    df_WhatIf_Total = (result["df_ejeX_Normal"].merge(df_LineaBase_WhatIf, on=["Ejex", "Filtro01", "Filtro02"], how="left").merge(df_Real_WhatIf, on=["Ejex", "Filtro01", "Filtro02"], how="left"))
    df_WhatIf_Total.fillna(0, inplace=True)
    df_WhatIf_Total["hh_lb_cum"] = (df_WhatIf_Total.groupby(["Filtro01", "Filtro02"])["hh_lb"].cumsum())
    df_WhatIf_Total["hh_real_cum"] = (df_WhatIf_Total.groupby(["Filtro01", "Filtro02"])["hh_real"].cumsum())
    
    df_WhatIf_Ajustada = (result["df_ejeX_Ajustada"].merge(df_LineaBase_Ajustada_WhatIf, on=["Ejex", "Filtro01", "Filtro02"], how="left").merge(df_Real_Ajustada_WhatIf, on=["Ejex", "Filtro01", "Filtro02"], how="left"))
    df_WhatIf_Ajustada.fillna(0, inplace=True)
    df_WhatIf_Ajustada["hh_lb_cum"] = (df_WhatIf_Ajustada.groupby(["Filtro01", "Filtro02"])["hh_lb"].cumsum())
    df_WhatIf_Ajustada["hh_real_cum"] = (df_WhatIf_Ajustada.groupby(["Filtro01", "Filtro02"])["hh_real"].cumsum())
    
    
    return {
        "df_LineaWhatIf_Total": df_WhatIf_Total,
        "df_LineaWhatIf_Ajustada": df_WhatIf_Ajustada
    }

def Calculo_Totales (df_LineaBase, df_Real, df_LineaBase_Ajustada, df_Real_Ajustada):
    
    if df_LineaBase["hh_lb_cum"].max() == 0:
        AvanceReal = 0
    else:
        AvanceReal = df_Real["hh_real_cum"].max()/df_LineaBase["hh_lb_cum"].max() 
        
    if df_LineaBase_Ajustada["hh_lb_cum"].max() == 0:
        AvanceRealAjustado = 0
    else:
        AvanceRealAjustado = df_Real_Ajustada["hh_real_cum"].max()/df_LineaBase_Ajustada["hh_lb_cum"].max() 
    
    return {
        "AvanceReal": AvanceReal,
        "AvanceAjustado": AvanceRealAjustado
    }

def Linea_Avance_RC(df_LineaBase, df_Real, df_LineaBase_Ajustada, df_Real_Ajustada):
    
    df_LineaBase = df_LineaBase[df_LineaBase['rutacritica']=="Si"]
    df_LineaBase_0 = df_LineaBase.groupby('Ejex')['hh_lb'].sum().reset_index()
    df_LineaBase_0["hh_lb_cum"] = df_LineaBase_0["hh_lb"].cumsum()

    df_LineaBase_Ajustada_0 = df_LineaBase_Ajustada.groupby('Ejex')['hh_lb'].sum().reset_index()
    #df_LineaBase_Ajustada_0["hh_lb_cum"] = df_LineaBase_Ajustada_0["hh_lb"].cumsum() ####################################
    df_LineaBase_Ajustada_0.rename(columns={'hh':'hh_lb'}, inplace=True)
    
    df_Real = df_Real[df_Real['rutacritica']=="Si"]
    df_Real_0 = df_Real.groupby('Ejex')['hh'].sum().reset_index()
    #df_Real_0["hh_real_cum"] = df_Real_0["hh"].cumsum() ############################################
    df_Real_0.rename(columns={'hh':'hh_real'}, inplace=True)
    
    df_Real_Ajustada_0 = df_Real.groupby('Ejex')['hh'].sum().reset_index()
    df_Real_Ajustada_0["hh_real_cum"] = df_Real_0["hh_real"].cumsum()
    #df_Real_Ajustada_0.rename(columns={'hh':'hh_real'}, inplace=True) 
    
    result = Rango_Eje_X(df_LineaBase_0, df_Real_0, df_LineaBase_Ajustada_0, df_Real_Ajustada_0)
    
    #Uniendos los dataframes de linea base y linea real y renombrando las columnas
    df_LineaGeneral = result["df_ejeX_Normal"].merge(df_LineaBase_0, on="Ejex", how="left").merge(df_Real_0, on="Ejex", how="left")
    df_LineaGeneral.fillna({"hh_lb": 0, "hh_real": 0}, inplace=True)

    df_LineaGeneral["hh_lb_cum"] = df_LineaGeneral["hh_lb"].cumsum() ############################################
    df_LineaGeneral["hh_real_cum"] = df_LineaGeneral["hh_real"].cumsum() ############################################
    #df_LineaGeneral.fillna({"hh_lb": 0, "hh_real": 0, "hh_lb_cum": 0, "hh_real_cum": 0}, inplace=True)

    #Uniendos los dataframes de linea base ajustada y linea real y renombrando las columnas
    df_LineaGeneral_Ajustada = result["df_ejeX_Ajustada"].merge(df_LineaBase_Ajustada_0, on="Ejex", how="left").merge(df_Real_0, on="Ejex", how="left")
    df_LineaGeneral_Ajustada.fillna({"hh_lb": 0, "hh_real": 0}, inplace=True)

    df_LineaGeneral_Ajustada["hh_lb_cum"] = df_LineaGeneral_Ajustada["hh_lb"].cumsum() ############################################
    df_LineaGeneral_Ajustada["hh_real_cum"] = df_LineaGeneral_Ajustada["hh_real"].cumsum() ############################################
    #df_LineaGeneral_Ajustada.fillna({"hh_lb": 0, "hh_real": 0, "hh_lb_cum": 0, "hh_real_cum": 0}, inplace=True)

    df_Real_0["hh_real_cum"] = df_Real_0["hh_real"].cumsum()
    df_LineaBase_Ajustada_0["hh_lb_cum"] = df_LineaBase_Ajustada_0["hh_lb"].cumsum()

    #Avances = Calculo_Totales(df_LineaBase_0,df_Real_0,df_LineaBase_Ajustada_0,df_Real_Ajustada_0)
    
    return{
        "df_LineaRC_Total": df_LineaGeneral,
        "df_LineaRC_Ajustada": df_LineaGeneral_Ajustada,
    }

def Linea_Avance_BloqueRC(df_LineaBase, df_Real, df_LineaBase_Ajustada, df_Real_Ajustada):
    print("Creando el dataframe de curva base area ajustada")

    df_LineaBase_Ajustada = df_LineaBase_Ajustada[df_LineaBase_Ajustada["BloqueRC"].notna()]
    df_LineaBase_Area = df_LineaBase.groupby(["Ejex","BloqueRC"])["hh_lb"].sum().reset_index()
    df_LineaBase_Area.sort_values(["BloqueRC", "Ejex"], inplace=True)
    df_LineaBase_Area.rename(columns={"BloqueRC":"Filtro01"}, inplace=True)
    
    df_LineaBase_Area_Ajustada = df_LineaBase_Ajustada.groupby(["Ejex","BloqueRC"])["hh_lb"].sum().reset_index()
    df_LineaBase_Area_Ajustada.sort_values(["BloqueRC", "Ejex"], inplace=True)
    df_LineaBase_Area_Ajustada.rename(columns={"BloqueRC":"Filtro01"}, inplace=True)
    

    df_Real = df_Real[df_Real["BloqueRC"].notna()]
    df_Real_Area = df_Real.groupby(["Ejex","BloqueRC"])["hh"].sum().reset_index()
    df_Real_Area.sort_values(["BloqueRC", "Ejex"], inplace=True)
    df_Real_Area.rename(columns={"hh":"hh_real"}, inplace=True)
    df_Real_Area.rename(columns={"BloqueRC":"Filtro01"}, inplace=True)
    
    df_Real_Area_Ajustada = df_Real_Ajustada.groupby(["Ejex","BloqueRC"])["hh"].sum().reset_index()
    df_Real_Area_Ajustada.sort_values(["BloqueRC", "Ejex"], inplace=True)
    df_Real_Area_Ajustada.rename(columns={"hh":"hh_real"}, inplace=True)
    df_Real_Area_Ajustada.rename(columns={"BloqueRC":"Filtro01"}, inplace=True)
    
    result = Rango_Eje_X_Area(df_LineaBase_Area, df_Real_Area, df_LineaBase_Area_Ajustada, df_Real_Area_Ajustada)
    
        
    df_LineaArea_Total = (result["df_ejeX_Normal"].merge(df_LineaBase_Area, on=["Ejex", "Filtro01"], how="left").merge(df_Real_Area, on=["Ejex", "Filtro01"], how="left"))
    df_LineaArea_Total.fillna(0, inplace=True)
    
        
    df_LineaArea_Total["hh_lb_cum"] = (
    df_LineaArea_Total
    .groupby("Filtro01")["hh_lb"]
    .cumsum()
    )

    df_LineaArea_Total["hh_real_cum"] = (
        df_LineaArea_Total
        .groupby("Filtro01")["hh_real"]
        .cumsum()
    )
    
    df_LineaArea_Ajustada = (result["df_ejeX_Ajustada"].merge(df_LineaBase_Area_Ajustada, on=["Ejex", "Filtro01"], how="left").merge(df_Real_Area_Ajustada, on=["Ejex", "Filtro01"], how="left"))
    df_LineaArea_Ajustada.fillna(0, inplace=True)
    
    df_LineaArea_Ajustada["hh_lb_cum"] = (
    df_LineaArea_Ajustada
    .groupby("Filtro01")["hh_lb"]
    .cumsum()
    )

    df_LineaArea_Ajustada["hh_real_cum"] = (
        df_LineaArea_Ajustada
        .groupby("Filtro01")["hh_real"]
        .cumsum()
    )
    
    return {
        "df_LineaBloqueRC_Total": df_LineaArea_Total,
        "df_LineaBloqueRC_Ajustada": df_LineaArea_Ajustada
    }


def Linea_Avance_Especialidad(df_LineaBase, df_Real, df_LineaBase_Ajustada, df_Real_Ajustada):


    df_LineaBase_Especialidad = df_LineaBase.groupby(["Ejex","especialidad"])["hh_lb"].sum().reset_index()


    df_LineaBase_Especialidad.sort_values(["especialidad", "Ejex"], inplace=True)
    df_LineaBase_Especialidad.rename(columns={"especialidad":"Filtro01"}, inplace=True)
    
    df_LineaBase_Especialidad_Ajustada = df_LineaBase_Ajustada.groupby(["Ejex","especialidad"])["hh_lb"].sum().reset_index()
    df_LineaBase_Especialidad_Ajustada.sort_values(["especialidad", "Ejex"], inplace=True)
    df_LineaBase_Especialidad_Ajustada.rename(columns={"especialidad":"Filtro01"}, inplace=True)
    
    df_Real_Especialidad = df_Real.groupby(["Ejex","especialidad"])["hh"].sum().reset_index() # Creo que debeería cambiar el HH por el EV
    #df_Real_Especialidad = df_Real.groupby(["Ejex","contratista"])["EV"].sum().reset_index() # Creo que debeería cambiar el HH por el EV

    
    df_Real_Especialidad.sort_values(["especialidad", "Ejex"], inplace=True)
    df_Real_Especialidad.rename(columns={"hh":"hh_real"}, inplace=True)
    #df_Real_Especialidad.rename(columns={"EV":"hh_real"}, inplace=True)
    df_Real_Especialidad.rename(columns={"especialidad":"Filtro01"}, inplace=True)

    
    df_Real_Especialidad_Ajustada = df_Real_Ajustada.groupby(["Ejex","especialidad"])["hh"].sum().reset_index()
    df_Real_Especialidad_Ajustada.sort_values(["especialidad", "Ejex"], inplace=True)
    df_Real_Especialidad_Ajustada.rename(columns={"hh":"hh_real"}, inplace=True)
    df_Real_Especialidad_Ajustada.rename(columns={"especialidad":"Filtro01"}, inplace=True)
    
    result = Rango_Eje_X_Contratista(df_LineaBase_Especialidad, df_Real_Especialidad, df_LineaBase_Especialidad_Ajustada, df_Real_Especialidad_Ajustada)
    
    df_LineaEspecialidad_Total = (result["df_ejeX_Normal"].merge(df_LineaBase_Especialidad, on=["Ejex", "Filtro01"], how="left").merge(df_Real_Especialidad, on=["Ejex", "Filtro01"], how="left"))
    df_LineaEspecialidad_Total.fillna(0, inplace=True)

    
    df_LineaEspecialidad_Total["hh_lb_cum"] = (
    df_LineaEspecialidad_Total
    .groupby("Filtro01")["hh_lb"]
    .cumsum()
    )

    df_LineaEspecialidad_Total["hh_real_cum"] = (
        df_LineaEspecialidad_Total
        .groupby("Filtro01")["hh_real"]
        .cumsum()
    )

 
        
    df_LineaEspecialidad_Ajustada = (result["df_ejeX_Ajustada"].merge(df_LineaBase_Especialidad_Ajustada, on=["Ejex", "Filtro01"], how="left").merge(df_Real_Especialidad_Ajustada, on=["Ejex", "Filtro01"], how="left"))
    df_LineaEspecialidad_Ajustada.fillna(0, inplace=True)
    
    df_LineaEspecialidad_Ajustada["hh_lb_cum"] = (
    df_LineaEspecialidad_Ajustada
    .groupby("Filtro01")["hh_lb"]
    .cumsum()
    )

    df_LineaEspecialidad_Ajustada["hh_real_cum"] = (
        df_LineaEspecialidad_Ajustada
        .groupby("Filtro01")["hh_real"]
        .cumsum()
    )
    
    return {
        "df_LineaEspecialidad_Total": df_LineaEspecialidad_Total,
        "df_LineaEspecialidad_Ajustada": df_LineaEspecialidad_Ajustada
    }



async def Process_Curvas_S ():

    
    result = await Linea_Base()
    df_LineaBase = result["df_LineaBase"]
    df_LineaBase_Ajustada = result["df_LineaBase_Ajustada"]
    df_Real = result["df_Real"]
    df_Real_Ajustada = result["df_Real_Ajustada"]
    
    df_Real = df_Real[df_Real['inicioreal'].notnull()].copy()

    df_Real["inicioreal"] = df_Real["inicioreal"].apply(
    lambda x: x - timedelta(hours=5) if pd.notnull(x) else x
    )

    df_Real["finreal"] = df_Real["finreal"].apply(
        lambda x: x - timedelta(hours=5) if pd.notnull(x) else x
    )

    # print(df_Real)

    if len(df_Real) >0: 
        #df_Real["TimeReference"] = (pd.to_datetime('now', utc=True).tz_localize(None))

        df_Real["TimeReference"] = (pd.to_datetime('now', utc=True).tz_localize(None) - timedelta(hours=5))

        df_Real["TimeReference"] = pd.to_datetime('2025-12-05')  #|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||

        df_Real['TimeReference'] = df_Real['TimeReference'].dt.ceil('h')

        df_Real['inicioreal'] = df_Real['inicioreal'].dt.ceil('h')


        df_Real["finreal"] = df_Real["finreal"].fillna(df_Real["TimeReference"]).dt.ceil('h')

        df_Real["DifHorasHorasNoRounded_Plan"] = ((df_Real["finplan"] - df_Real["inicioplan"]).dt.total_seconds() / 3600)

        df_Real["DifHorasHoras_Plan"] = ((df_Real["finplan"] - df_Real["inicioplan"]).dt.total_seconds() / 3600).apply(math.ceil)
        df_Real["DifHorasTimePlan"] = ((df_Real["finplan"].dt.ceil('h') - df_Real["inicioplan"].dt.ceil('h')).dt.total_seconds() / 3600).apply(math.ceil)
        df_Real["DifHorasTime"] = abs((df_Real["finreal"] - df_Real["inicioreal"]))
        df_Real["DifHorasHoras"] = abs(((df_Real["finreal"] - df_Real["inicioreal"]).dt.total_seconds() / 3600).apply(math.ceil))

        df_Real["DifHorasHoras"] = df_Real["DifHorasHoras"].replace(0,1)

        df_Real["hh"] = df_Real["DifHorasHorasNoRounded_Plan"] / df_Real["DifHorasHoras"]


        
        #df_Real['Ejex'] = df_Real.apply(lambda row: [row['inicioreal'] + timedelta(hours=i) for i in range(row['DifHorasHoras'] )], axis=1)
        df_Real['Ejex'] = df_Real.apply(lambda row: [row['inicioreal'] + timedelta(hours=i) for i in range(0, row['DifHorasHoras'], -1)] if row['DifHorasHoras'] < 0 else [row['inicioreal'] + timedelta(hours=i) for i in range(row['DifHorasHoras'])],axis=1)

     
        
        df_Real = df_Real.explode('Ejex')
        # print("df_Real: ")
        # print(df_Real["inicioreal"])
        # print(df_Real)
        df_Real['Ejex'] = df_Real['Ejex'].dt.ceil('h')
        df_Real["EV"] = df_Real["hh"]*df_Real["avance"]/100
        df_Real["hh"] = df_Real["EV"]

 


        df_Real_Ajustada = df_Real_Ajustada[df_Real_Ajustada['inicioreal'].notnull()].copy()
        df_Real_Ajustada["TimeReference"] = pd.to_datetime('now', utc=True).tz_localize(None)
        df_Real_Ajustada['TimeReference'] = df_Real_Ajustada['TimeReference'].dt.ceil('h')
        df_Real_Ajustada['inicioreal'] = df_Real_Ajustada['inicioreal'].dt.ceil('h')
        df_Real_Ajustada["finreal"] = df_Real_Ajustada["finreal"].fillna(df_Real_Ajustada["TimeReference"]).dt.ceil('h')
        df_Real_Ajustada["DifHorasTime"] = (df_Real_Ajustada["finreal"] - df_Real_Ajustada["inicioreal"])
        df_Real_Ajustada["DifHorasHoras"] = ((df_Real_Ajustada["finreal"] - df_Real_Ajustada["inicioreal"]).dt.total_seconds() / 3600).apply(math.ceil)
        #df_Real_Ajustada['Ejex'] = df_Real_Ajustada.apply(lambda row: [row['inicioreal'] + timedelta(hours=i) for i in range(row['DifHorasHoras'] )], axis=1)
        df_Real_Ajustada['Ejex'] = df_Real_Ajustada.apply(lambda row: [row['inicioreal'] + timedelta(hours=i) for i in range(0, row['DifHorasHoras'], -1)] if row['DifHorasHoras'] < 0 else [row['inicioreal'] + timedelta(hours=i) for i in range(row['DifHorasHoras'])],axis=1)

        df_Real_Ajustada = df_Real_Ajustada.explode('Ejex')
        df_Real_Ajustada['Ejex'] = df_Real_Ajustada['Ejex'].dt.ceil('h')
    else:
        df_Real = pd.DataFrame()
    
    result_Linea_General = Linea_Avance_General(df_LineaBase, df_Real, df_LineaBase_Ajustada, df_Real_Ajustada)
    result_Linea_Area = Linea_Avance_Area(df_LineaBase, df_Real, df_LineaBase_Ajustada, df_Real_Ajustada)
    result_Linea_Contratista = Linea_Avance_Contratista(df_LineaBase, df_Real, df_LineaBase_Ajustada, df_Real_Ajustada)
    result_Linea_Especialidad = Linea_Avance_Especialidad(df_LineaBase, df_Real, df_LineaBase_Ajustada, df_Real_Ajustada)
    result_Linea_Area_Contratista = Linea_Avance_Area_Contratista(df_LineaBase, df_Real, df_LineaBase_Ajustada, df_Real_Ajustada)
    result_Linea_Especialidad_Contratista = Linea_Avance_Contratista_Especialidad(df_LineaBase, df_Real, df_LineaBase_Ajustada, df_Real_Ajustada)
    result_Linea_WhatIf = Linea_Avance_WhatIf(df_LineaBase, df_Real, df_LineaBase_Ajustada, df_Real_Ajustada)
    result_Linea_RC = Linea_Avance_RC(df_LineaBase, df_Real, df_LineaBase_Ajustada, df_Real_Ajustada)
    result_Linea_BloqueRC = Linea_Avance_BloqueRC(df_LineaBase, df_Real, df_LineaBase_Ajustada, df_Real_Ajustada)

    
    
    
    return {
        "df_CurvaTotal": result_Linea_General["df_LineaGeneral"],
        "df_CurvaTotalAjustada": result_Linea_General["df_LineaGeneral_Ajustada"],
        
        "df_CurvaArea_Total": result_Linea_Area["df_LineaArea_Total"],
        "df_CurvaArea_Ajustada": result_Linea_Area["df_LineaArea_Ajustada"],
        
        "df_CurvaContratista_Total": result_Linea_Contratista["df_LineaContratista_Total"],
        "df_CurvaContratista_Ajustada": result_Linea_Contratista["df_LineaContratista_Ajustada"],
        
        "df_CurvaAreaContratista_Total": result_Linea_Area_Contratista["df_LineaAreaContratista_Total"],
        "df_CurvaAreaContratista_Ajustada": result_Linea_Area_Contratista["df_LineaAreaContratista_Ajustada"],
        
        "df_CurvaEspecialidadContratista_Total": result_Linea_Especialidad_Contratista["df_LineaEspecialidadContratista_Total"],
        "df_CurvaEspecialidadContratista_Ajustada": result_Linea_Especialidad_Contratista["df_LineaEspecialidadContratista_Ajustada"],

        "df_CurvaRC_Total": result_Linea_RC["df_LineaRC_Total"],
        "df_CurvaRC_Ajustada": result_Linea_RC["df_LineaRC_Ajustada"],
        
        "df_CurvaBloqueRC_Total": result_Linea_BloqueRC["df_LineaBloqueRC_Total"],
        "df_CurvaBloqueRC_Ajustada": result_Linea_BloqueRC["df_LineaBloqueRC_Ajustada"],


        "df_CurvaEspecialidad_Total": result_Linea_Especialidad["df_LineaEspecialidad_Total"],
        "df_CurvaEspecialidad_Ajustada": result_Linea_Especialidad["df_LineaEspecialidad_Ajustada"],
    }

async def Process_Curva_WhatIf (area, contratista):
    print("Procesando WhatIf")

    result = await Linea_Base()
    df_LineaBase = result["df_LineaBase"]
    df_LineaBase_Ajustada = result["df_LineaBase_Ajustada"]
    df_Real = result["df_Real"]
    df_Real_Ajustada = result["df_Real_Ajustada"]
    
    if area:
        df_LineaBase = df_LineaBase[df_LineaBase['area'].isin(area)]
        df_LineaBase_Ajustada = df_LineaBase_Ajustada[df_LineaBase_Ajustada['area'].isin(area)]
        df_Real = df_Real[df_Real['area'].isin(area)]
        df_Real_Ajustada = df_Real_Ajustada[df_Real_Ajustada['area'].isin(area)]
    if contratista:
        df_LineaBase = df_LineaBase.copy()
        df_LineaBase["FiltroConcatenado"] = df_LineaBase["contratista"] + "-" + df_LineaBase["especialidad"]
        df_LineaBase_Ajustada = df_LineaBase_Ajustada.copy()
        df_LineaBase_Ajustada["FiltroConcatenado"] = df_LineaBase_Ajustada["contratista"] + "-" + df_LineaBase_Ajustada["especialidad"]
        df_Real = df_Real.copy()
        df_Real["FiltroConcatenado"] = df_Real["contratista"] + "-" + df_Real["especialidad"]
        df_Real_Ajustada = df_Real_Ajustada.copy()
        df_Real_Ajustada["FiltroConcatenado"] = df_Real_Ajustada["contratista"] + "-" + df_Real_Ajustada["especialidad"]
        df_LineaBase = df_LineaBase[df_LineaBase['FiltroConcatenado'].isin(contratista)]
        df_LineaBase_Ajustada = df_LineaBase_Ajustada[df_LineaBase_Ajustada['FiltroConcatenado'].isin(contratista)]
        df_Real = df_Real[df_Real['FiltroConcatenado'].isin(contratista)]
        df_Real_Ajustada = df_Real_Ajustada[df_Real_Ajustada['FiltroConcatenado'].isin(contratista)]
    
    df_Real = df_Real[df_Real['inicioreal'].notnull()].copy()
    df_Real["TimeReference"] = pd.to_datetime('now', utc=True).tz_localize(None)

    df_Real['TimeReference'] = df_Real['TimeReference'].dt.ceil('h')
    df_Real['inicioreal'] = df_Real['inicioreal'].dt.ceil('h')
    df_Real["finreal"] = df_Real["finreal"].fillna(df_Real["TimeReference"]).dt.ceil('h')
    df_Real["DifHorasTime"] = abs(df_Real["finreal"] - df_Real["inicioreal"])
    df_Real["DifHorasHoras"] = abs(((df_Real["finreal"] - df_Real["inicioreal"]).dt.total_seconds() / 3600).apply(math.ceil))
    #df_Real['Ejex'] = df_Real.apply(lambda row: [row['inicioreal'] + timedelta(hours=i) for i in range(row['DifHorasHoras'] )], axis=1)
    df_Real['Ejex'] = df_Real.apply(lambda row: [row['inicioreal'] + timedelta(hours=i) for i in range(0, row['DifHorasHoras'], -1)] if row['DifHorasHoras'] < 0 else [row['inicioreal'] + timedelta(hours=i) for i in range(row['DifHorasHoras'])],axis=1)

    df_Real = df_Real.explode('Ejex')
    df_Real['Ejex'] = df_Real['Ejex'].dt.ceil('h')
    
    
    df_Real_Ajustada = df_Real_Ajustada[df_Real_Ajustada['inicioreal'].notnull()].copy()
    df_Real_Ajustada["TimeReference"] = pd.to_datetime('now', utc=True).tz_localize(None)
    df_Real_Ajustada['TimeReference'] = df_Real_Ajustada['TimeReference'].dt.ceil('h')
    df_Real_Ajustada['inicioreal'] = df_Real_Ajustada['inicioreal'].dt.ceil('h')
    df_Real_Ajustada["finreal"] = df_Real_Ajustada["finreal"].fillna(df_Real_Ajustada["TimeReference"]).dt.ceil('h')
    df_Real_Ajustada["DifHorasTime"] = abs((df_Real_Ajustada["finreal"] - df_Real_Ajustada["inicioreal"]))
    df_Real_Ajustada["DifHorasHoras"] = abs(((df_Real_Ajustada["finreal"] - df_Real_Ajustada["inicioreal"]).dt.total_seconds() / 3600).apply(math.ceil))
    df_Real_Ajustada['Ejex'] = df_Real_Ajustada.apply(lambda row: [row['inicioreal'] + timedelta(hours=i) for i in range(row['DifHorasHoras'] )], axis=1)
    df_Real_Ajustada['Ejex'] = df_Real_Ajustada.apply(lambda row: [row['inicioreal'] + timedelta(hours=i) for i in range(0, row['DifHorasHoras'], -1)] if row['DifHorasHoras'] < 0 else [row['inicioreal'] + timedelta(hours=i) for i in range(row['DifHorasHoras'])],axis=1)

    df_Real_Ajustada = df_Real_Ajustada.explode('Ejex')
    df_Real_Ajustada['Ejex'] = df_Real_Ajustada['Ejex'].dt.ceil('h')
    
    df_LineaBase = df_LineaBase.copy()
    
    df_LineaBase["Contratista-Especialidad"] = df_LineaBase["contratista"] + "-" + df_LineaBase["especialidad"]

    df_LineaBase.rename(columns={"Contratista-Especialidad":"Filtro01", "area":"Filtro02"}, inplace=True)
    
    df_filtros_unicos = df_LineaBase[["Filtro01", "Filtro02"]].drop_duplicates().reset_index(drop=True)

    
    #result_Linea_WhatIf = Linea_Avance_WhatIf(df_LineaBase, df_Real, df_LineaBase_Ajustada, df_Real_Ajustada)
    result_Linea_WhatIf = Linea_Avance_General(df_LineaBase, df_Real, df_LineaBase_Ajustada, df_Real_Ajustada)
    
    result_Linea_WhatIf["df_LineaGeneral"] = pd.concat([result_Linea_WhatIf["df_LineaGeneral"],df_filtros_unicos],ignore_index=True)
    result_Linea_WhatIf["df_LineaGeneral_Ajustada"] = pd.concat([result_Linea_WhatIf["df_LineaGeneral_Ajustada"],df_filtros_unicos],ignore_index=True)
    

    return {
        "df_WhatIf_Total": result_Linea_WhatIf["df_LineaGeneral"],
        "df_WhatIf_Ajustada": result_Linea_WhatIf["df_LineaGeneral_Ajustada"]
    }



@router.get("/ProcesarLineaBase", tags=["Parada de Planta"])
async def Get_Process_BaseLine ():
    df_result = []  
    df_processed = await Process_Curvas_S()
    df_processedWhatIf = await Process_Curva_WhatIf(None, None)
    
    print(df_processed["df_CurvaEspecialidad_Total"])

    dataframes = {

        "CurvaGeneral": {
        "General": df_processed["df_CurvaTotal"],
        "Ajustada": df_processed["df_CurvaTotalAjustada"],
        },
        "CurvaArea": {
            "General": df_processed["df_CurvaArea_Total"],
            "Ajustada": df_processed["df_CurvaArea_Ajustada"],
        },
        "CurvaContratista": {
            "General": df_processed["df_CurvaContratista_Total"],
            "Ajustada": df_processed["df_CurvaContratista_Ajustada"],
        },
        "CurvaAreaContratista": {
            "General": df_processed["df_CurvaAreaContratista_Total"],
            "Ajustada": df_processed["df_CurvaAreaContratista_Ajustada"],
        },
        "CurvaEspecialidadContratista": {
            "General": df_processed["df_CurvaEspecialidadContratista_Total"],
            "Ajustada": df_processed["df_CurvaEspecialidadContratista_Ajustada"],
        },
        # "CurvaWhatIf": {
        #     "General": df_processedWhatIf["df_WhatIf_Total"],
        #     "Ajustada": df_processedWhatIf["df_WhatIf_Ajustada"],
        # },
        "CurvaRC": {
            "General": df_processed["df_CurvaRC_Total"],
            "Ajustada": df_processed["df_CurvaRC_Ajustada"],
        },
        "CurvaBloqueRC": {
            "General": df_processed["df_CurvaBloqueRC_Total"],
            "Ajustada": df_processed["df_CurvaBloqueRC_Ajustada"],
        },
        "CurvaEspecialidad": {
            "General": df_processed["df_CurvaEspecialidad_Total"],
            "Ajustada": df_processed["df_CurvaEspecialidad_Ajustada"],
        },
    }
    
    function_return_Streaming(dataframes,df_result)
    print("Enviando datos al frontend")
    return StreamingResponse(df_result[0], media_type='application/json')


@router.get("/ProcesarWhatIf", tags=["Parada de Planta"])
async def Process_WhatIf (area: Optional[List[str]] = Query(None),contratista: Optional[List[str]] = Query(None)):
    df_result = []  
    df_processed = await Process_Curva_WhatIf(area, contratista)
    


    dataframes = {

        "CurvaWhatIf": {
        "General": df_processed["df_WhatIf_Total"],
        "Ajustada": df_processed["df_WhatIf_Ajustada"],
        }
    
    }
    
    function_return_Streaming(dataframes,df_result)
    print("Enviando datos al frontend")
    return StreamingResponse(df_result[0], media_type='application/json')


