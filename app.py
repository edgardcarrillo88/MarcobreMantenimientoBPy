from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from api.v1.costos import router as v1_router
from api.v1.indicadores import router as v2_router

app = FastAPI()

origins = [
    "http://localhost:8000", 
    "https://marcobremantenimientobpy-production.up.railway.app",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    )

app.include_router(v1_router,prefix="/api/v1/costos", tags=["Costos"])
app.include_router(v2_router,prefix="/api/v1/indicadores", tags=["Indicadores"])

