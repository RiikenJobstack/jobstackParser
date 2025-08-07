# user_service.py
import os
from typing import Optional, Dict, Any
from motor.motor_asyncio import AsyncIOMotorClient
from bson import ObjectId
from bson.errors import InvalidId
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseConnection:
    _instance = None
    _client = None
    _database = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DatabaseConnection, cls).__new__(cls)
        return cls._instance

    def get_config(self) -> Dict[str, str]:
        env = os.getenv("NODE_ENV", "development").lower()

        config = {
            "development": {
                "uri": os.getenv("MONGO_DB_URL") or os.getenv("DATABASE_URL_DEV"),
                "database": "jobstack"
            },
            "uat": {
                "uri": os.getenv("MONGO_DB_URL_UAT"),
                "database": "jobstack"
            },
            "production": {
                "uri": os.getenv("MONGO_DB_URL_PROD"),
                "database": "jobstack-prod"
            }
        }.get(env)

        if not config or not config["uri"]:
            raise ValueError(f"Missing DB config or URI for environment: {env}")

        logger.info(f"🌐 Using environment: {env}")
        logger.info(f"🗄️  Connecting to DB: {config['database']}")
        return config

    async def initialize(self):
        if self._client is None:
            try:
                config = self.get_config()
                self._client = AsyncIOMotorClient(config["uri"])
                await self._client.admin.command("ping")
                self._database = self._client[config["database"]]
                logger.info("✅ MongoDB connected successfully")
            except Exception as e:
                logger.error(f"❌ MongoDB connection failed: {e}")
                raise e

    async def get_database(self):
        if self._database is None:
            await self.initialize()
        return self._database

    async def close(self):
        if self._client:
            self._client.close()
            logger.info("🔌 MongoDB connection closed")
            self._client = None
            self._database = None


db_connection = DatabaseConnection()

async def get_database():
    return await db_connection.get_database()


async def find_user_by_id(user_id: str) -> Optional[Dict[str, Any]]:
    try:
        db = await get_database()
        collection = db.get_collection("usersv2")

        try:
            object_id = ObjectId(user_id)
        except InvalidId:
            logger.warning(f"⚠️ Invalid ObjectId: {user_id}")
            return None

        user = await collection.find_one({"_id": object_id})
        if user:
            user["_id"] = str(user["_id"])
            return user
        return None
    except Exception as e:
        logger.error(f"❌ Error finding user {user_id}: {e}")
        return None


# FastAPI lifecycle
async def startup_database():
    await db_connection.initialize()

async def shutdown_database():
    await db_connection.close()
