from abc import ABC, abstractmethod
from typing import Any
import pandas as pd
from pymongo import MongoClient
from bson import json_util
import os


class Storage(ABC):
    """
    Abstract class representing a storage.
    """

    @abstractmethod
    def connect(self, credentials: Any) -> Any:
        """
        Establishes connection with a storage.
        """
        pass

    @abstractmethod
    def close(self, conn: Any) -> None:
        """
        Closes the existing connection with a storage.
        """
        pass

    @abstractmethod
    def write(self, conn: Any, data: pd.DataFrame, location: str) -> None:
        """
        Exports data to the storage in the specified location.
        """
        pass

    @abstractmethod
    def read(self, conn: Any, location: str) -> pd.DataFrame:
        """
        Imports data from the storage located in the specified place.
        """
        pass


class StorageJSON(Storage):
    """
    JSON storage class.
    """

    def connect(self, credentials: Any) -> Any:
        if not os.path.exists(credentials):
            os.mkdir(credentials)
        return credentials

    def close(self, conn: Any) -> None:
        return None

    def write(self, conn: Any, data: pd.DataFrame, location: str) -> None:
        data['date'] = data['date'].dt.strftime('%Y-%m-%d')
        with open(conn + location + '.json', 'w', encoding='UTF-8') as f:
            f.write(data.to_json(orient='records'))

    def read(self, conn: Any, location: str) -> pd.DataFrame:
        with open(conn + location + '.json', 'r', encoding='UTF-8') as f:
            json_str = f.read()
        data = pd.DataFrame(json_util.loads(json_str))
        return data


class StorageMongo(Storage):
    """
    MongoDB storage class.
    """

    def connect(self, credentials: Any) -> Any:
        username = credentials['username']
        password = credentials['password']
        url = credentials['url']
        mongo_uri = f"mongodb+srv://{username}:{password}{url}"

        return MongoClient(mongo_uri)

    def close(self, conn: Any) -> None:
        conn.close()

    def write(self, conn: Any, data: pd.DataFrame, location: str) -> None:
        data['date'] = data['date'].dt.strftime('%Y-%m-%d')
        db = conn["moex"]
        if location in db.list_collection_names():
            collection = db[location]
            collection.drop()
        collection = db[location]
        collection.insert_many(json_util.loads(data.to_json(orient='records')))
    
    def read(self, conn: Any, location: str) -> pd.DataFrame:
        db = conn['moex']
        if location not in db.list_collection_names():
            raise ValueError(f'No collection with name {location} was found!')
        collection = db[location]
        data = pd.DataFrame(list(collection.find()))
        data = data.drop('_id', axis=1)
        return data
