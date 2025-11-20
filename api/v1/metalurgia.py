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


        
async def id_to_string_process(cursor, array):
    async for item in cursor:
        item['_id'] = str(item['_id']) 
        array.append(item)
    return array



@router.get("/ProcesarInputs", tags=["Metalurgia"])
async def Get_Procesar_Inputs ():


    All_Data_Inputs = []
    CursorInputs = db.Metalurgia_Input.find({})

    
    print("Procesando los Inputs de Metalurgia")
    await id_to_string_process(CursorInputs,All_Data_Inputs)
    df_Inputs = pd.DataFrame(All_Data_Inputs)

        
    print("Procesando frame Inputs")
    df_Inputs["Percentage_CuR"] = df_Inputs["Percentage_CuT"].astype(float) - df_Inputs["Percentage_CuAS"].astype(float) - df_Inputs["Percentage_CuCN"].astype(float)
    df_Inputs["Met_CuT"] = df_Inputs["TMS"].astype(float) * df_Inputs["Percentage_CuT"].astype(float)/100
    df_Inputs["Met_CuAS"] = df_Inputs["TMS"].astype(float) * df_Inputs["Percentage_CuAS"].astype(float)/100
    df_Inputs["Met_CuCN"] = df_Inputs["TMS"].astype(float) * df_Inputs["Percentage_CuCN"].astype(float)/100
    df_Inputs["Met_CuRes"] = df_Inputs["TMS"].astype(float) * df_Inputs["Percentage_CuR"].astype(float)/100
    df_Inputs["Met_Fe"] = df_Inputs["TMS"].astype(float) * df_Inputs["Percentage_Fe"].astype(float)/100
    df_Inputs["Met_S"] = df_Inputs["TMS"].astype(float) * df_Inputs["Percentage_S"].astype(float)/100



#Todo este bloque se puede vectorizar
############################################
    print("Procesando OTYPE")
    Condiciones_OTYPE = [
    {"cond": lambda row: row["Percentage_CuAS"] / row["Percentage_CuT"] >= 0.3,"result": 14},
    {"cond": lambda row: (row["Percentage_CuCN"] / row["Percentage_CuT"] >= 0.5) and (row["Percentage_S"] >= 0.5),"result": 31},
    {"cond": lambda row: (row["Percentage_CuCN"] / row["Percentage_CuT"] >= 0.5) and (row["Percentage_S"] < 0.5),"result": 32},
    {"cond": lambda row: (row["Percentage_CuR"] / row["Percentage_CuT"] >= 0.6) and (row["Percentage_CuT"] / row["Percentage_S"] >= 0.3), "result": 33},
    {"cond": lambda row: (row["Percentage_CuR"] / row["Percentage_CuT"] >= 0.6) and (row["Percentage_CuT"] / row["Percentage_S"] < 0.3),  "result": 34},
]
    DEFAULT = 41
    ERROR_VALUE = 0



    def Condicional_OTYPE(row):
        for item in Condiciones_OTYPE:
            if item["cond"](row):
                return item["result"]
        return DEFAULT

    df_Inputs["OTYPE"] = df_Inputs.apply(Condicional_OTYPE, axis=1)
    df_Inputs["OTYPE"] = df_Inputs["OTYPE"].astype(int)

############################################




#Condiciones Tipo de Sulfuros
############################################
    print("Procesando Tipo de Sulfuros")
    condiciones_TipoSulf = [
        (df_Inputs.OTYPE == 14) & (df_Inputs.Percentage_CuT > 0.8),   # -> "SUL8"
        (df_Inputs.OTYPE == 14) & (df_Inputs.Percentage_CuT <= 0.8),  # -> "SUL5"
        (df_Inputs.OTYPE == 31) & (df_Inputs.Percentage_CuT > 0.8),   # -> "SUL9"
        (df_Inputs.OTYPE == 32) & (df_Inputs.Percentage_CuT > 0.8),   # -> "SUL9"
        (df_Inputs.OTYPE == 31) & (df_Inputs.Percentage_CuT <= 0.8),  # -> "SUL6"
        (df_Inputs.OTYPE == 32) & (df_Inputs.Percentage_CuT <= 0.8),  # -> "SUL6"
        (df_Inputs.OTYPE == 33) & (df_Inputs.Percentage_CuT > 0.8),   # -> "SUL10"
        (df_Inputs.OTYPE == 41) & (df_Inputs.Percentage_CuT > 0.8),   # -> "SUL10"
        (df_Inputs.OTYPE == 33) & (df_Inputs.Percentage_CuT <= 0.8),  # -> "SUL7"
        (df_Inputs.OTYPE == 41) & (df_Inputs.Percentage_CuT <= 0.8),  # -> "SUL7"
        (df_Inputs.OTYPE == 34)                                    # -> "SUL11"
]
    
    valores = [
        "SUL8","SUL5","SUL9","SUL9","SUL6","SUL6","SUL10","SUL10","SUL7","SUL7","SUL11"
    ]

    df_Inputs["TipoSulf"] = np.select(condiciones_TipoSulf, valores, default="")

############################################




#****************************MODELO REC_CUT_NML****************************

#Ratios
##############################################

    print("Procesando Ratios")
    df_Inputs["Ratio_Cu_Fe"] = df_Inputs["Percentage_CuT"].replace(0,np.nan).divide(df_Inputs["Percentage_Fe"]).fillna(0)
    df_Inputs["Ratio_Cu_S"] = df_Inputs["Percentage_CuT"].replace(0,np.nan).divide(df_Inputs["Percentage_S"]).fillna(0)
    df_Inputs["Ratio_Fe_S"] = np.where(df_Inputs["Percentage_CuT"]==0,0,(df_Inputs["Percentage_Fe"]/df_Inputs["Percentage_S"]))
    df_Inputs["Ratio_Ratox"] = np.where(df_Inputs["Percentage_CuT"]==0,0,(df_Inputs["Percentage_CuAS"]*100/(df_Inputs["Percentage_CuT"])))
    df_Inputs["Ratio_RCuCN"] = np.where(df_Inputs["Percentage_CuT"]==0,0,df_Inputs["Percentage_CuCN"]*100/(df_Inputs["Percentage_CuT"]))
    df_Inputs["Ratio_Rcures"] = np.where(df_Inputs["Percentage_CuT"]==0,0,df_Inputs["Percentage_CuR"]*100/(df_Inputs["Percentage_CuT"]))

##############################################






#Capping
##############################################

    print("Procesando Capping")
    #SUL05
    df_Inputs["Capping_CuT_SUL05"] = df_Inputs["Percentage_CuT"].clip(lower=0.22, upper=2.12)
    df_Inputs.loc[df_Inputs["Percentage_CuT"] == 0, "Capping_CuT_SUL05"] = 0


    #SUL06
    df_Inputs["Capping_CuT_SUL06"] = df_Inputs["Percentage_CuT"].clip(lower=0.22, upper=0.79)
    df_Inputs.loc[df_Inputs["Percentage_CuT"] == 0, "Capping_CuT_SUL06"] = 0

    df_Inputs["Capping_CuAs_SUL06"] = df_Inputs["Percentage_CuAS"].clip(lower=0.02, upper=0.21)
    df_Inputs.loc[df_Inputs["Percentage_CuT"] == 0, "Capping_CuAs_SUL06"] = 0

    df_Inputs["Capping_CuCN_SUL06"] = df_Inputs["Percentage_CuCN"].clip(lower=0.12, upper=0.59)
    df_Inputs.loc[df_Inputs["Percentage_CuT"] == 0, "Capping_CuAs_SUL06"] = 0

    df_Inputs["Capping_Fe_SUL06"] = df_Inputs["Percentage_Fe"].clip(lower=5.05, upper=9.58)
    df_Inputs.loc[df_Inputs["Percentage_CuT"] == 0, "Capping_Fe_SUL06"] = 0

    df_Inputs["Capping_Cu_Fe_SUL06"] = df_Inputs["Ratio_Cu_Fe"].clip(lower=0.04, upper=0.1)
    df_Inputs.loc[df_Inputs["Percentage_CuT"] == 0, "Capping_Fe_SUL06"] = 0


    #SUL07
    df_Inputs["Capping_CuT_SUL07"] = df_Inputs["Percentage_CuT"].clip(lower=0.28, upper=0.70)
    df_Inputs.loc[df_Inputs["Percentage_CuT"] == 0, "Capping_CuT_SUL07"] = 0

    df_Inputs["Capping_CuCN_SUL07"] = df_Inputs["Percentage_CuCN"].clip(lower=0.02, upper=0.28)
    df_Inputs.loc[df_Inputs["Percentage_CuT"] == 0, "Capping_CuCN_SUL07"] = 0

    df_Inputs["Capping_CuR_SUL07"] = df_Inputs["Percentage_CuR"].clip(lower=0.08, upper=0.49)
    df_Inputs.loc[df_Inputs["Percentage_CuT"] == 0, "Capping_CuR_SUL07"] = 0

    df_Inputs["Capping_S_SUL07"] = df_Inputs["Percentage_S"].clip(lower=0.3, upper=0.98)
    df_Inputs.loc[df_Inputs["Percentage_CuT"] == 0, "Capping_S_SUL07"] = 0


    #SUL08
    df_Inputs["Capping_CuT_SUL08"] = df_Inputs["Percentage_CuT"].clip(lower=0.22, upper=2.12)
    df_Inputs.loc[df_Inputs["Percentage_CuT"] == 0, "Capping_CuT_SUL08"] = 0


    #SUL10
    df_Inputs["Capping_CuT_SUL10"] = df_Inputs["Percentage_CuT"].clip(lower=0.88, upper=6.71)
    df_Inputs.loc[df_Inputs["Percentage_CuT"] == 0, "Capping_CuT_SUL10"] = 0

    df_Inputs["Capping_CuAs_SUL10"] = df_Inputs["Percentage_CuAS"].clip(lower=0.03, upper=0.493)
    df_Inputs.loc[df_Inputs["Percentage_CuT"] == 0, "Capping_CuAs_SUL10"] = 0

    df_Inputs["Capping_CuR_SUL10"] = df_Inputs["Percentage_CuR"].clip(lower=0.45, upper=3.08)
    df_Inputs.loc[df_Inputs["Percentage_CuT"] == 0, "Capping_CuR_SUL10"] = 0

    df_Inputs["Capping_Cu_S_SUL10"] = df_Inputs["Ratio_Cu_S"].clip(lower=0.52, upper=1.26)
    df_Inputs.loc[df_Inputs["Percentage_CuT"] == 0, "Capping_Cu_S_SUL10"] = 0


    #SUL11
    df_Inputs["Capping_CuT_SUL11"] = df_Inputs["Percentage_CuT"].clip(lower=0.1, upper=3.72)
    df_Inputs.loc[df_Inputs["Percentage_CuT"] == 0, "Capping_CuT_SUL11"] = 0

    df_Inputs["Capping_CuAs_SUL11"] = df_Inputs["Percentage_CuAS"].clip(lower=0.01, upper=0.12)
    df_Inputs.loc[df_Inputs["Percentage_CuT"] == 0, "Capping_CuAs_SUL11"] = 0

    df_Inputs["Capping_CuR_SUL11"] = df_Inputs["Percentage_CuR"].clip(lower=0.07, upper=3.48)
    df_Inputs.loc[df_Inputs["Percentage_CuT"] == 0, "Capping_CuR_SUL11"] = 0

    df_Inputs["Capping_Fe_SUL11"] = df_Inputs["Percentage_Fe"].clip(lower=8.11, upper=60.39)
    df_Inputs.loc[df_Inputs["Percentage_CuT"] == 0, "Capping_Fe_SUL11"] = 0

    df_Inputs["Capping_S_SUL11"] = df_Inputs["Percentage_S"].clip(lower=0.55, upper=18.48)
    df_Inputs.loc[df_Inputs["Percentage_CuT"] == 0, "Capping_S_SUL11"] = 0

##############################################







#Modelo Recuperación
##############################################

    print("Procesando Modelos de Recuperación")
    df_Inputs["Rec_Cu_Ro_est_Sulf05"] = np.where(df_Inputs["Percentage_CuT"]==0,0, 144438-1444.25*df_Inputs["Ratio_Ratox"]-1442.95*df_Inputs["Ratio_RCuCN"]-1443.47*df_Inputs["Ratio_Rcures"]-16.9225*df_Inputs["Capping_CuT_SUL05"])*0.97
    df_Inputs["Rec_Cu_Ro_est_Sulf06"] = np.where(df_Inputs["Percentage_CuT"]==0,0, 50.6647-38.0122*df_Inputs["Capping_CuT_SUL06"]-51.4038*df_Inputs["Capping_CuAs_SUL06"]-52.0046*df_Inputs["Capping_CuCN_SUL06"]+4.76163*df_Inputs["Capping_Fe_SUL06"]+668.496*df_Inputs["Capping_Cu_Fe_SUL06"])*0.97
    df_Inputs["Rec_Cu_Ro_est_Sulf07"] = np.where(df_Inputs["Percentage_CuT"]==0,0, 93.4242+41.5092*df_Inputs["Capping_CuT_SUL07"]-93.0249*df_Inputs["Capping_CuCN_SUL07"]-0.630505*df_Inputs["Capping_CuR_SUL07"]-19.3877*df_Inputs["Capping_S_SUL07"])*0.97
    df_Inputs["Rec_Cu_Ro_est_Sulf08"] = np.where(df_Inputs["Percentage_CuT"]==0,0, 144438-1444.25*df_Inputs["Ratio_Ratox"] -1442.95*df_Inputs["Ratio_RCuCN"]-1443.47*df_Inputs["Ratio_Rcures"]-16.9225*df_Inputs["Capping_CuT_SUL08"])*0.97
    df_Inputs["Rec_Cu_Ro_est_Sulf10"] = np.where(df_Inputs["Percentage_CuT"]==0,0, 90.9576+1.91432*df_Inputs["Capping_CuT_SUL10"]-38.6173*df_Inputs["Capping_CuAs_SUL10"]+0.407034*df_Inputs["Capping_CuR_SUL10"]+4.78678*df_Inputs["Capping_Cu_S_SUL10"])*0.97
    df_Inputs["Rec_Cu_Ro_est_Sulf11"] = np.where(df_Inputs["Percentage_CuT"]==0,0, 87.9213-7.13017*df_Inputs["Capping_CuT_SUL11"]+69.606*df_Inputs["Capping_CuAs_SUL11"]+10.8435*df_Inputs["Capping_CuR_SUL11"]+0.135969*df_Inputs["Capping_Fe_SUL11"]-0.95964*df_Inputs["Capping_S_SUL11"])*0.97

##############################################







#Ajuste Modelo Recuperación
##############################################

    print("Procesando Ajuste de Modelos de Recuperación")
    Ajuste_Sulf05 = df_Inputs.loc[df_Inputs["TipoSulf"] == "SUL5", "Rec_Cu_Ro_est_Sulf05"].quantile(0.8)
    print("Ajuste_Sulf05: ", Ajuste_Sulf05)

    Ajuste_Sulf07 = df_Inputs.loc[df_Inputs["TipoSulf"] == "SUL7", "Rec_Cu_Ro_est_Sulf07"].quantile(0.9)
    print("Ajuste_Sulf07: ", Ajuste_Sulf07)

    Ajuste_Sulf11 = df_Inputs.loc[df_Inputs["TipoSulf"] == "SUL11", "Rec_Cu_Ro_est_Sulf11"].quantile(0.9)
    print("Ajuste_Sulf11: ", Ajuste_Sulf11)

    df_Inputs["Rec_Capping_Sulf05"] = np.where(df_Inputs["Rec_Cu_Ro_est_Sulf05"]>=Ajuste_Sulf05,Ajuste_Sulf05,df_Inputs["Rec_Cu_Ro_est_Sulf05"])
    df_Inputs["Rec_Capping_Sulf07"] = np.where(df_Inputs["Rec_Cu_Ro_est_Sulf07"]>=Ajuste_Sulf07,Ajuste_Sulf07,df_Inputs["Rec_Cu_Ro_est_Sulf07"])
    df_Inputs["Rec_Capping_Sulf11"] = np.where(df_Inputs["Rec_Cu_Ro_est_Sulf11"]>=Ajuste_Sulf11,Ajuste_Sulf11,df_Inputs["Rec_Cu_Ro_est_Sulf11"])

    

##############################################




#Output Recuperación CuT
##############################################

    print("Procesando Output Recuperación CuT   ")
    Condiciones_Rec_CuT = [
        df_Inputs.TipoSulf == "SUL5",
        df_Inputs.TipoSulf == "SUL6",
        df_Inputs.TipoSulf == "SUL7",
        df_Inputs.TipoSulf == "SUL8",
        df_Inputs.TipoSulf == "SUL10",
        df_Inputs.TipoSulf == "SUL11"
    ]

    valores_Rec_CuT = [
        df_Inputs["Rec_Capping_Sulf05"],df_Inputs["Rec_Cu_Ro_est_Sulf06"],df_Inputs["Rec_Capping_Sulf07"],df_Inputs["Rec_Cu_Ro_est_Sulf08"],df_Inputs["Rec_Cu_Ro_est_Sulf10"],df_Inputs["Rec_Capping_Sulf11"]
    ]



    df_Inputs["Percentage_Rec_CuT"] = np.select(Condiciones_Rec_CuT,valores_Rec_CuT,default=0)



#****************************MODELO REC_CUT_knn****************************


#scaling_params_knn
##############################################
    print("definición de scaling_params_knn")

    scaling_params_knn = [
        {"Variable":"head_cus_pct","Promedio":0.225004347596828,"Desviacion_Estandar":0.0714328742343645},
        {"Variable":"head_fe_pct","Promedio":13.33627332,"Desviacion_Estandar":4.23869682},
        {"Variable":"cu_qc","Promedio":1.580248965,"Desviacion_Estandar":0.414686793},
        {"Variable":"cus_cut","Promedio":0.153284306,"Desviacion_Estandar":0.069133719},
        {"Variable":"cucn_cut","Promedio":0.634555506,"Desviacion_Estandar":0.200758555},
        {"Variable":"fet_cut","Promedio":9.045167162,"Desviacion_Estandar":3.939042462},
        {"Variable":"ag_fet","Promedio":1.438277751,"Desviacion_Estandar":0.703789175}
    ]

    All_Data_Calibracion = []
    CursorCalibracion = db.Metalurgia_Calibracion.find({})

    
    print("Procesando los datos de calibración de Metalurgia")
    await id_to_string_process(CursorCalibracion,All_Data_Calibracion)
    df_Calibracion = pd.DataFrame(All_Data_Calibracion)

    df_Calibracion["esc_cus"] = (df_Calibracion["head_cus_pct"] - scaling_params_knn[0]["Promedio"]) / scaling_params_knn[0]["Desviacion_Estandar"]
    df_Calibracion["esc_fe"] = (df_Calibracion["head_fe_pct"] - scaling_params_knn[1]["Promedio"]) / scaling_params_knn[1]["Desviacion_Estandar"]
    df_Calibracion["esc_cut"] = (df_Calibracion["cu_qc"] - scaling_params_knn[2]["Promedio"]) / scaling_params_knn[2]["Desviacion_Estandar"]
    df_Calibracion["esc_cus_cut"] = (df_Calibracion["cus_cut"] - scaling_params_knn[3]["Promedio"]) / scaling_params_knn[3]["Desviacion_Estandar"]
    df_Calibracion["esc_cucn_cut"] = (df_Calibracion["cucn_cut"] - scaling_params_knn[4]["Promedio"]) / scaling_params_knn[4]["Desviacion_Estandar"]
    df_Calibracion["esc_fet_cut"] = (df_Calibracion["fet_cut"] - scaling_params_knn[5]["Promedio"]) / scaling_params_knn[5]["Desviacion_Estandar"]
    df_Calibracion["esc_ag_fet"] = (df_Calibracion["ag_fet"] - scaling_params_knn[6]["Promedio"]) / scaling_params_knn[6]["Desviacion_Estandar"]


		
##############################################

	
	
		


#Condiciones Rec CuT - Modelo
##############################################


 


    # VAL_CA5 = "modelo_nml_CA5"
    # VAL_CB5 = "modelo_nml_CB5"
    # VAL_CC5 = "modelo_nml_CC5"
    # VAL_BV5 = "modelo_nml_BV5"
    # VAL_Y2  = "modelo_knn_Y2"

    # condiciones_Rec_CuT = [
    #     (df_Inputs.TipoSulf == "SUL5"),                                             # → VAL_CA5
    #     (df_Inputs.TipoSulf == "SUL6"),                                             # → VAL_Y2
    #     (df_Inputs.TipoSulf == "SUL7"),                                             # → VAL_CB5
    #     (df_Inputs.TipoSulf == "SUL9"),                                             # → VAL_Y2
    #     (df_Inputs.TipoSulf == "SUL10"),                                            # → VAL_Y2
    #     (df_Inputs.TipoSulf == "SUL11"),                                            # → VAL_CC5
    #     (df_Inputs.TipoSulf == "SUL8") & (df_Inputs.Percentage_CuT >= 1.1),         # → VAL_Y2
    #     (df_Inputs.TipoSulf == "SUL8") & (df_Inputs.Percentage_CuT < 1.1),          # → VAL_BV5
    # ]

    
    # valores_Rec_CuT_Final = [
    #     VAL_CA5,   # SUL5
    #     VAL_Y2,    # SUL6
    #     VAL_CB5,   # SUL7
    #     VAL_Y2,    # SUL9
    #     VAL_Y2,    # SUL10
    #     VAL_CC5,   # SUL11
    #     VAL_Y2,    # SUL8 con CuT >= 1.1
    #     VAL_BV5,   # SUL8 con CuT < 1.1
    # ]

    # df_Inputs["Rec_CuT_Modelo"] = np.select(condiciones_Rec_CuT, valores_Rec_CuT_Final, default="No Aplica")

##############################################


    print("Print de Inputs")
    print(df_Inputs)

    print("Print de Calibracion")
    print(df_Calibracion)

    return {"Message": "Oki Doki"}


