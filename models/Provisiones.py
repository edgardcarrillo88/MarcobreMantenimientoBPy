from pydantic import BaseModel
from typing import Optional
from datetime import datetime


class Provisiones (BaseModel):
    _id: Optional[str] = None
    ClaseCosto: Optional[str]=None
    DescClaseCosto: Optional[str]=None
    CeCo: Optional[str]=None
    DescCeCo: Optional[str]=None
    CodProveedor: Optional[str]=None
    NombreProveedor: Optional[str]=None
    FechaEnvioProvision: Optional[str]=None
    FechaEjecucionServicio: Optional[str]=None
    OC: Optional[str]=None
    Posicion: Optional[str]=None
    NoEDP: Optional[str]=None
    VersionEDP: Optional[str]=None
    Glosa: Optional[str]=None
    Monto: Optional[float]=None
    Moneda: Optional[str]=None
    Status: Optional[str]=None
    Responsable: Optional[str]=None
    Correo: Optional[str]=None
    Partida: Optional[str]=None
    DescripcionPartida: Optional[str]=None
    DescripcionServicio: Optional[str]=None
    SolicitudReprovision: Optional[str]=None
    Pagado: Optional[str]=None
    Planta: Optional[str]=None
    TipoProvision: Optional[str]=None
    deleted: Optional[bool] = None
    createdAt: Optional[datetime] = None
    updatedAt: Optional[datetime] =  None