"""
database.py
------------
MongoDB layer for the File-to-Link / Private Storage Bot.
Uses Motor (async MongoDB driver) so all calls are non-blocking.

Collections:
    users        -> unique user tracking for stats & broadcast
    links        -> single-file / batch link records
    files        -> individual file docs (linked to a link_id, supports batching)
"""

import os
import time
import string
import random
import logging
from typing import Optional, List, Dict, Any

from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("database")

MONGO_URI = os.environ.get("MONGO_URI")
DB_NAME = os.environ.get("DB_NAME", "file_storage_bot")

if not MONGO_URI:
    raise RuntimeError("MONGO_URI is not set in the environment (.env file).")


class Database:
    def __init__(self, uri: str, db_name: str):
        self.client = AsyncIOMotorClient(uri)
        self.db = self.client[db_name]

        self.users = self.db.users
        self.links = self.db.links
        self.files = self.db.files

    # ---------------------------------------------------------------
    # Helpers
    # ---------------------------------------------------------------
    @staticmethod
    def generate_id(length: int = 8) -> str:
        """Generate a short random alphanumeric ID for links/batches."""
        chars = string.ascii_letters + string.digits
        return "".join(random.choices(chars, k=length))

    async def ensure_indexes(self):
        """Create indexes for performance. Call once at startup."""
        try:
            await self.users.create_index("user_id", unique=True)
            await self.links.create_index("link_id", unique=True)
            await self.files.create_index("link_id")
        except Exception as e:
            logger.warning(f"Index creation warning: {e}")

    # ---------------------------------------------------------------
    # USER TRACKING (for stats + broadcast)
    # ---------------------------------------------------------------
    async def add_user(self, user_id: int, username: Optional[str] = None) -> bool:
        """Add a user if not already present. Returns True if newly added."""
        existing = await self.users.find_one({"user_id": user_id})
        if existing:
            return False
        await self.users.insert_one(
            {
                "user_id": user_id,
                "username": username,
                "joined_at": time.time(),
            }
        )
        return True

    async def is_user_present(self, user_id: int) -> bool:
        return await self.users.find_one({"user_id": user_id}) is not None

    async def get_all_users(self) -> List[int]:
        cursor = self.users.find({}, {"user_id": 1, "_id": 0})
        return [doc["user_id"] async for doc in cursor]

    async def total_users_count(self) -> int:
        return await self.users.count_documents({})

    async def remove_user(self, user_id: int):
        """Remove a user (e.g. if they blocked the bot) during broadcast cleanup."""
        await self.users.delete_one({"user_id": user_id})

    # ---------------------------------------------------------------
    # LINKS (single file or batch container)
    # ---------------------------------------------------------------
    async def create_link(
        self,
        link_id: str,
        admin_id: int,
        caption: Optional[str] = None,
        password: Optional[str] = None,
        is_batch: bool = False,
    ) -> Dict[str, Any]:
        doc = {
            "link_id": link_id,
            "admin_id": admin_id,
            "caption": caption,
            "password": password,
            "is_batch": is_batch,
            "clicks": 0,
            "revoked": False,
            "created_at": time.time(),
        }
        await self.links.insert_one(doc)
        return doc

    async def get_link(self, link_id: str) -> Optional[Dict[str, Any]]:
        return await self.links.find_one({"link_id": link_id})

    async def set_password(self, link_id: str, password: str) -> bool:
        result = await self.links.update_one(
            {"link_id": link_id}, {"$set": {"password": password}}
        )
        return result.modified_count > 0

    async def remove_password(self, link_id: str) -> bool:
        result = await self.links.update_one(
            {"link_id": link_id}, {"$set": {"password": None}}
        )
        return result.modified_count > 0

    async def revoke_link(self, link_id: str) -> bool:
        result = await self.links.update_one(
            {"link_id": link_id}, {"$set": {"revoked": True}}
        )
        return result.modified_count > 0

    async def increment_click(self, link_id: str):
        await self.links.update_one({"link_id": link_id}, {"$inc": {"clicks": 1}})

    async def top_links(self, limit: int = 10) -> List[Dict[str, Any]]:
        cursor = self.links.find({}).sort("clicks", -1).limit(limit)
        return [doc async for doc in cursor]

    async def total_links_count(self) -> int:
        return await self.links.count_documents({})

    # ---------------------------------------------------------------
    # FILES (each file references a link_id; a batch has many files
    # under the same link_id)
    # ---------------------------------------------------------------
    async def add_file(
        self,
        link_id: str,
        file_id: str,
        file_unique_id: str,
        file_type: str,
        caption: Optional[str] = None,
        file_name: Optional[str] = None,
        file_size: Optional[int] = None,
    ):
        doc = {
            "link_id": link_id,
            "file_id": file_id,
            "file_unique_id": file_unique_id,
            "file_type": file_type,  # photo / video / audio / document
            "caption": caption,
            "file_name": file_name,
            "file_size": file_size,
            "added_at": time.time(),
        }
        await self.files.insert_one(doc)
        return doc

    async def get_files_for_link(self, link_id: str) -> List[Dict[str, Any]]:
        cursor = self.files.find({"link_id": link_id})
        return [doc async for doc in cursor]

    async def count_files_for_link(self, link_id: str) -> int:
        return await self.files.count_documents({"link_id": link_id})

    async def delete_files_for_link(self, link_id: str):
        await self.files.delete_many({"link_id": link_id})


# Singleton instance used across the bot
db = Database(MONGO_URI, DB_NAME)