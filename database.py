import os
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

# Obtener la contraseña de MongoDB desde las variables de entorno
MONGO_DB_PASS = os.getenv("MONGO_DB_PASS")

# Crear el cliente de MongoDB con la conexión
client = AsyncIOMotorClient(f"mongodb+srv://CALA:{MONGO_DB_PASS}@cluster0.wxzehsb.mongodb.net/Project?retryWrites=true&w=majority")

# Selecciona la base de datos
db = client.Project
