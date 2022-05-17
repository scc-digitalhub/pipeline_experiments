"""
This module contains a collection of tasks for interacting with MongoDB databases via
the pymongo library.
"""
from typing import Any
from pymongo import MongoClient
from pymongo.cursor import Cursor

import prefect
from prefect import Task
from prefect.utilities.tasks import defaults_from_attrs

class MongoRead(Task):
    """
    Task for reading documents from a MongoDB collection.
    Args:
        - host (str): database host address
        - port (int, optional): port used to connect to MongoDB, defaults to 27017
        - db_name (str): name of MongoDB database
        - collection_name (str): name of collection
        - **kwargs (Any, optional): additional keyword arguments to pass to the Task constructor
    """
    def __init__(
        self,
        host: str = None,
        port: int = 27017,
        db_name: str = None,
        collection_name: str = None,
        **kwargs
    ):
        self.host = host
        self.port = port
        self.db_name = db_name
        self.collection_name = collection_name
        super().__init__(**kwargs)

    @defaults_from_attrs("host", "port", "db_name", "collection_name")
    def run(
        self,
        host: str = None,
        port: int = 27017,
        db_name: str = None,
        collection_name: str = None,
    ) -> Cursor:
        if not collection_name:
            raise ValueError("A collection name must be provided")

        client = MongoClient(host=host, port=port)

        try:
            with client:
                #get or create Database obj
                db = client[db_name]
                #return a pymongo.cursor.Cursor
                cursor = db[collection_name].find()

            self.logger.info("Returning cursor for: {}".format(collection_name))
            return cursor

        except (Exception, TypeError) as e:
            raise e

        #ensure connection is closed
        finally:
            client.close()

class MongoEstimatedCount(Task):
    """
    Task for getting an estimated count of documents in a MongoDB collection.
    Args:
        - host (str): database host address
        - port (int, optional): port used to connect to MongoDB, defaults to 27017
        - db_name (str): name of MongoDB database
        - collection_name (str): name of collection
        - **kwargs (Any, optional): additional keyword arguments to pass to the Task constructor
    """
    def __init__(
        self,
        host: str = None,
        port: int = 27017,
        db_name: str = None,
        collection_name: str = None,
        **kwargs
    ):
        self.host = host
        self.port = port
        self.db_name = db_name
        self.collection_name = collection_name
        super().__init__(**kwargs)

    @defaults_from_attrs("host", "port", "db_name", "collection_name")
    def run(
        self,
        host: str = None,
        port: int = 27017,
        db_name: str = None,
        collection_name: str = None,
    ) -> int:
        if not collection_name:
            raise ValueError("A collection name must be provided")

        client = MongoClient(host=host, port=port)

        try:
            with client:
                #get or create Database obj
                db = client[db_name]
                #NOTE: using collection metadata instead of counting docs one by one, result may be incorrect
                #e.g. in case of sharded clusters, unclean shutdown
                res = db[collection_name].estimated_document_count()

            self.logger.info("Estimated doc count for collection {} is {}".format(collection_name, res))
            return res

        except Exception as e:
            raise e

        #ensure connection is closed
        finally:
            client.close()

class MongoGetOne(Task):
    """
    Task for getting one document from a MongoDB collection.
    Args:
        - host (str): database host address
        - port (int, optional): port used to connect to MongoDB, defaults to 27017
        - db_name (str): name of MongoDB database
        - collection_name (str): name of collection
        - filter (dict): query document
        - **kwargs (Any, optional): additional keyword arguments to pass to the Task constructor
    """
    def __init__(
        self,
        host: str = None,
        port: int = 27017,
        db_name: str = None,
        collection_name: str = None,
        filter: dict = None,
        **kwargs
    ):
        self.host = host
        self.port = port
        self.db_name = db_name
        self.collection_name = collection_name
        self.filter = filter
        super().__init__(**kwargs)

    @defaults_from_attrs("host", "port", "db_name", "collection_name", "filter")
    def run(
        self,
        host: str = None,
        port: int = 27017,
        db_name: str = None,
        collection_name: str = None,
        filter: dict = None,
    ) -> dict:
        if not collection_name:
            raise ValueError("A collection name must be provided")

        if not filter:
            raise ValueError("A filter query document must be provided")

        client = MongoClient(host=host, port=port)

        try:
            with client:
                #get or create Database obj
                db = client[db_name]
                res = db[collection_name].find_one(filter)

            self.logger.info("Returning: {} for filter query {}".format(res, filter))
            return res

        except (Exception, TypeError) as e:
            raise e

        #ensure connection is closed
        finally:
            client.close()

class MongoContains(Task):
    """
    Task for checking if a MongoDB collection contains a document.
    Args:
        - host (str): database host address
        - port (int, optional): port used to connect to MongoDB, defaults to 27017
        - db_name (str): name of MongoDB database
        - collection_name (str): name of collection
        - filter (dict): query document
        - **kwargs (Any, optional): additional keyword arguments to pass to the Task constructor
    """
    def __init__(
        self,
        host: str = None,
        port: int = 27017,
        db_name: str = None,
        collection_name: str = None,
        filter: dict = None,
        **kwargs
    ):
        self.host = host
        self.port = port
        self.db_name = db_name
        self.collection_name = collection_name
        self.filter = filter
        super().__init__(**kwargs)

    @defaults_from_attrs("host", "port", "db_name", "collection_name", "filter")
    def run(
        self,
        host: str = None,
        port: int = 27017,
        db_name: str = None,
        collection_name: str = None,
        filter: dict = None,
    ) -> bool:
        if not collection_name:
            raise ValueError("A collection name must be provided")

        if not filter:
            raise ValueError("A filter query document must be provided")

        client = MongoClient(host=host, port=port)

        try:
            with client:
                #get or create Database obj
                db = client[db_name]
                res = db[collection_name].find_one(filter=filter, projection=["_id"])

            self.logger.info("Found: {} in collection {}".format(res, collection_name))
            return res != None

        except (Exception, TypeError) as e:
            raise e

        #ensure connection is closed
        finally:
            client.close()

class MongoInsertOrReplaceOne(Task):
    """
    Task for inserting or replacing one document in a MongoDB collection.
    Args:
        - host (str): database host address
        - port (int, optional): port used to connect to MongoDB, defaults to 27017
        - db_name (str): name of MongoDB database
        - collection_name (str): name of collection
        - filter (dict): query document
        - data (Any): new document
        - **kwargs (Any, optional): additional keyword arguments to pass to the Task constructor
    """
    def __init__(
        self,
        host: str = None,
        port: int = 27017,
        db_name: str = None,
        collection_name: str = None,
        filter: dict = None,
        data: Any = None,
        **kwargs
    ):
        self.host = host
        self.port = port
        self.db_name = db_name
        self.collection_name = collection_name
        self.filter = filter
        self.data = data
        super().__init__(**kwargs)

    @defaults_from_attrs("host", "port", "db_name", "collection_name", "filter", "data")
    def run(
        self,
        host: str = None,
        port: int = 27017,
        db_name: str = None,
        collection_name: str = None,
        filter: dict = None,
        data: Any = None,
    ) -> None:
        if not collection_name:
            raise ValueError("A collection name must be provided")

        if not filter:
            raise ValueError("A filter query document must be provided")

        if not data:
            raise ValueError("Data for replacement must be provided")

        client = MongoClient(host=host, port=port)

        try:
            with client:
                #get or create Database obj
                db = client[db_name]
                res = db[collection_name].replace_one(filter, data, upsert=True)

            self.logger.info("Replaced: {}, inserted: {}".format(res.modified_count, res.upserted_id))

        except Exception as e:
            raise e

        #ensure connection is closed
        finally:
            client.close()
