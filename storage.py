from abc import ABC, abstractmethod
from typing import Any
import pandas as pd
from pymongo import MongoClient
import os


class Storage(ABC):

    @abstractmethod
    def connect(self, credentials: Any) -> Any:
        pass

    @abstractmethod
    def close(self, conn: Any) -> None:
        pass

    @abstractmethod
    def write(self, conn: Any, data: pd.DataFrame, location: str) -> None:
        pass


class StorageJSON(Storage):

    def connect(self, credentials: Any) -> Any:
        if not os.path.exists(credentials):
            os.mkdir(credentials)
        return credentials

    def close(self, conn: Any) -> None:
        return None

    def write(self, conn: Any, data: pd.DataFrame, location: str) -> None:
        with open(conn + location, 'w', encoding='UTF-8') as f:
            f.write(data.to_json(orient='records'))


class StorageMongo(Storage):

    def connect(self, credentials: Any) -> Any:
        username = credentials['username']
        password = credentials['password']
        url = credentials['url']
        mongo_uri = f"mongodb+srv://{username}:{password}{url}"

        return MongoClient(mongo_uri)

    def close(self, conn: Any) -> None:
        conn.close()

    def write(self, conn: Any, data: pd.DataFrame, location: str) -> None:
        db = conn["moex"]
        if location in db.list_collection_names():
            collection = db[location]
            collection.drop()
        collection = db[location]
        collection.insert(data.to_json(orient='records'))
