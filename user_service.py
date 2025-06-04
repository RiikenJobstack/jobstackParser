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

    async def initialize(self):
        """Initialize MongoDB connection"""
        if self._client is None:
            try:
                mongodb_url = os.getenv("MONGODB_URL")
                if not mongodb_url:
                    raise ValueError("MONGODB_URL environment variable is required")
                
                self._client = AsyncIOMotorClient(mongodb_url)
                
                # Test the connection
                await self._client.admin.command('ping')
                logger.info("Successfully connected to MongoDB")
                
                # Get database name from environment or use default
                db_name = os.getenv("DATABASE_NAME", "your_database_name")
                self._database = self._client[db_name]
                
            except Exception as e:
                logger.error(f"Failed to connect to MongoDB: {e}")
                raise e

    async def get_database(self):
        """Get database instance"""
        if self._database is None:
            await self.initialize()
        return self._database

    async def close(self):
        """Close MongoDB connection"""
        if self._client:
            self._client.close()
            self._client = None
            self._database = None
            logger.info("MongoDB connection closed")

# Global database connection instance
db_connection = DatabaseConnection()

async def get_database():
    """Get database instance"""
    return await db_connection.get_database()

async def find_user_by_id(user_id: str) -> Optional[Dict[str, Any]]:
    """
    Find user by ID in MongoDB
    
    Args:
        user_id (str): User ID to search for
        
    Returns:
        Optional[Dict[str, Any]]: User document if found, None otherwise
    """
    try:
        # Get database instance
        db = await get_database()
        print(f"Connecting to database: {db}")
        collections = await db.list_collection_names()
        print(f"Available collections: {collections}")

        # Get users collection (change 'users' to your actual collection name if different)
        # Common names: 'users', 'UserV2', 'user'
        users_collection = db.usersv2 # or db.UserV2 if that's your collection name
        
        # Convert string ID to ObjectId
        try:
            object_id = ObjectId(user_id)
        except InvalidId:
            logger.warning(f"Invalid ObjectId format: {user_id}")
            return None
        
        # Find user by _id
        user = await users_collection.find_one({"_id": object_id})
        
        if user:
            # Convert ObjectId to string for JSON serialization
            user["_id"] = str(user["_id"])
            logger.info(f"User found: {user_id}")
            return user
        else:
            logger.info(f"User not found: {user_id}")
            return None
            
    except Exception as e:
        logger.error(f"Database error while finding user {user_id}: {e}")
        return None

# Startup and shutdown event handlers for FastAPI
async def startup_database():
    """Initialize database connection on startup"""
    await db_connection.initialize()

async def shutdown_database():
    """Close database connection on shutdown"""
    await db_connection.close()