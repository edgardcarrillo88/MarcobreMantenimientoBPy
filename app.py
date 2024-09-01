from fastapi import FastAPI
from api.v1.costos import router as v1_router
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

origins = [
    "http://localhost:8000", 
    "https://marcobremantenimientobpy-production.up.railway.app/",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    )

app.include_router(v1_router,prefix="/api/v1")

