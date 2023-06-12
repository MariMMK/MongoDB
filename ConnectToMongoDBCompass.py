# _._._._._._._._._._._._.  import required libraried _._._._._._._._._._._._. 
from dotenv import load_dotenv, find_dotenv 
# a library is used to load environment variables from a .env file.
import os 
# a library is used to access environment variables.
import pprint
# a library is used for pretty printing.
from pymongo import MongoClient 
#  library provides the MongoDB driver for Python.



# _._._._._._._._._._._. Loading environment variables_._._._._._._._._._._._. 

load_dotenv (find_dotenv())
# function loads environment variables from the .env file.
password = os.environ.get ("MANGODB_PWD")
# is retrieved from the environment variables



# _._._._._._._._._._._.  Connecting to MongoDB _._._._._._._._._._._._.

connection_string = f"""mongodb+srv://marziehmohamadi:{password}@tutorial.gdx3xrv.mongodb.net/?retryWrites=true&w=majority"""
# using the MongoDB Atlas credentials :

client = MongoClient (connection_string)
#An instance of the MongoClient class is created with the connection string.


