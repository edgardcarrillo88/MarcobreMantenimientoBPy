from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class ActualPlanta(BaseModel):
    _id: Optional[str] = None
    Gerencia: Optional[str] = None
    Planta: Optional[str] = None
    Area: Optional[str] = None
    SubArea: Optional[str] = None
    Categoria: Optional[str] = None
    CeCo: Optional[str] = None
    DescripcionCeCo: Optional[str] = None
    ClaseCosto: Optional[str] = None
    DescripcionClaseCosto: Optional[str] = None
    Responsable: Optional[str] = None
    Especialidad: Optional[str] = None
    Partida: Optional[str] = None
    DescripcionPartida: Optional[str] = None
    CategoriaActual: Optional[str] = None
    Mes: Optional[int]=None
    Monto: Optional[float]=None
    PptoForecast: Optional[str] = None
    Proveedor: Optional[str] = None
    TxtPedido: Optional[str] = None
    Justificacion: Optional[str] = None
    CN: Optional[str] = None
    deleted: Optional[bool] = None
    createdAt: Optional[datetime] = None
    updatedAt: Optional[datetime] =  None
