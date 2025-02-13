from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime

class LaborModel(BaseModel):
    Mecanicos: float = 0.0
    Soldadores: float = 0.0
    Vigias: float = 0.0
    Electricista: float = 0.0
    Instrumentista: float = 0.0

class NoLaborModel(BaseModel):
    Andamios: bool = False
    CamionGrua: bool = False
    Telescopica: bool = False

class LineaBase(BaseModel):
    _id: Optional[str] = None
    deleted: Optional[bool] = None
    createdAt: Optional[datetime] = None
    updatedAt: Optional[datetime] =  None
    
    id: Optional[int] = None
    nivel: Optional[float] = None
    WBS: Optional[str] = None
    descripcion: Optional[str] = None
    OT:  Optional[str] = None
    TAG:  Optional[str] = None
    inicioplan:  Optional[str] = None
    finplan:  Optional[str] = None
    avance: Optional[float] = None
    estado:  Optional[str] = None
    responsable:  Optional[str] = None
    contratista:  Optional[str] = None
    especialidad:  Optional[str] = None
    BloqueRC:  Optional[str] = None
    comentarios:  Optional[str] = None
    inicioreal:  Optional[str] = None
    finreal:  Optional[str] = None
    area:  Optional[str] = None
    hh: Optional[float] = None,
    curva: Optional[str] = None
    lastupdate:  Optional[str] = None       
    rutacritica:  Optional[str] = None   
    ActividadCancelada:  Optional[str] = None
    SupResponsable:  Optional[str] = None
    Otros: Optional[str] = None
    
    Labor: LaborModel = LaborModel()
    NoLabor: NoLaborModel = NoLaborModel()

