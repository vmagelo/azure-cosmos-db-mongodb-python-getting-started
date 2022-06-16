import getpass
import pymongo
import os

from datetime import datetime
from random import randint
from dotenv import load_dotenv


# ----------------------------------------------------------------------------------------------------------
#  Prerequisites:
#
# 1. An Azure Cosmos DB API for MongoDB Account.
# 2. PyMongo installed.
# ----------------------------------------------------------------------------------------------------------
# Sample - demonstrates the basic CRUD operations on a document in the Azure Cosmos DB API for MongoDB
# ----------------------------------------------------------------------------------------------------------

# CONNECTION_STRING = getpass.getpass(prompt='Enter your primary connection string: ') # Prompts user for connection string
# CONNECTION_STRING = "mongodb://python-testing:bZ7QdJ8rFlKT1f3Is36952GTPooWURgqRnlsmO4i9cJQkZ1mUD26pBsvuBIm4sf9r6jCgrj56Ed6mDOlVCxC1g==@python-testing.mongo.cosmos.azure.com:10255/?ssl=true&retrywrites=false&replicaSet=globaldb&maxIdleTimeMS=120000&appName=@python-testing@"
# DB_NAME = "api-mongodb-sample-database"
# UNSHARDED_COLLECTION_NAME = "restaurants_reviews"

# Load environment variables from .env file
load_dotenv()
CONNECTION_STRING = os.getenv('CONNECTION_STRING')
DB_NAME = os.getenv('DB_NAME')
COLLECTION_NAME = os.getenv('COLLECTION_NAME')

SAMPLE_FIELD_NAME = "sample_field"

def delete_document(collection, document_id):
    """Delete the document containing document_id from the collection"""
    collection.delete_one({"_id": document_id})
    print("Deleted document with _id {}".format(document_id))

def read_document(collection, document_id):
    """Return the contents of the document containing document_id"""
    print("Found a document with _id {}: {}".format(document_id, collection.find_one({"_id": document_id})))

def update_document(collection, document_id):
    """Update the sample field value in the document containing document_id"""
    collection.update_one({"_id": document_id}, {"$set":{SAMPLE_FIELD_NAME: "Updated!"}})
    print("Updated document with _id {}: {}".format(document_id, collection.find_one({"_id": document_id})))

def insert_sample_document(collection):
    """Insert a sample document and return the contents of its _id field"""
    document_id = collection.insert_one({SAMPLE_FIELD_NAME: randint(50, 500)}).inserted_id
    print("Inserted document with _id {}".format(document_id))
    return document_id

def create_database_unsharded_collection(client):
    """Create sample database with shared throughput if it doesn't exist and an unsharded collection"""
    db = client[DB_NAME]

    # Create database if it doesn't exist
    if DB_NAME not in client.list_database_names():
        # Database with 400 RU throughput that can be shared across the DB's collections
        db.command({'customAction': "CreateDatabase", 'offerThroughput': 400})
        print("Created db {} with shared throughput". format(DB_NAME))
    
    # Create collection if it doesn't exist
    if COLLECTION_NAME not in db.list_collection_names():
        # Creates a unsharded collection that uses the DBs shared throughput
        db.command({'customAction': "CreateCollection", 'collection': COLLECTION_NAME})
        print("Created collection {}". format(COLLECTION_NAME))
        print("Collection name {}". format(db.COLLECTION_NAME))
    
    #return db.COLLECTION_NAME
    return db.get_collection(COLLECTION_NAME)
    
def main():
    """Connect to the API for MongoDB, create DB and collection, perform CRUD operations"""
    """Delete all documents"""
    client = pymongo.MongoClient(CONNECTION_STRING)
    try:
        client.server_info() # validate connection string
    except pymongo.errors.ServerSelectionTimeoutError:
        raise TimeoutError("Invalid API for MongoDB connection string or timed out when attempting to connect")

    db = client[DB_NAME]
    collection = db.get_collection(COLLECTION_NAME)
    collection.delete_many({})

if __name__ == '__main__':
    main()
