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



# _._._._._._._._._._._.  Database and Collection  _._._._._._._._._._._. 
# Access a specific database
dbs = client.list_database_names()
#print ("DataBases :" , dbs)
test_db = client ["test"]
collections =  test_db.list_collection_names()
#print ( "collections : " ,collections)



# _._._._._._._._._._._._.  Inserting the document _._._._._._._._._._._._. 

def insert_test_doc ():
    
    collection = test_db.test
    #      database name.collection name
    #define a new document as a python dictionary 
    test_document = {
        "name" : "Marzieh" ,
        "course" : "BDM1003 Big Data tool"
    }
    inserted_id = collection.insert_one(test_document).inserted_id
    print (inserted_id)

# insert_test_doc()  # Call the function 

# Creat new database and collection,  Insert multiple documentsat at once :
production = client.production  # new database
person_collection = production.person_collection # create new collection under database "production"

def create_documents(): #create a function to Insert multiple documentsat the same time
    first_names = ["Adriana" , "Robbi", "Megan", "Barak"]
    last_names = ["Smith", "Grey" , "Markel", "Obama"]
    ages = [32, 36, 40, 56]

    docs =[]

    for first_name, last_names, age in zip(first_names, last_names, ages):
        doc = {"first_name" : first_name, "last_name": last_names, "age": age}
        docs.append (doc)

    person_collection.insert_many(docs)

#create_documents()  # call the function 

# _._._._._._._._._._._._.  Reading a Document _._._._._._._._._._._._. 

printer = pprint.PrettyPrinter() # use this function of pprint to make the output nicer!

def find_all_people(): 
    people = person_collection.find() # .find is methods used to retrieve documents from the collection. if we put an argument in the () it find anything that match with the argument in database, if we put it empty, it gives us all the documnets.


    for person in people:
        printer.pprint (person)

# find_all_people()  # call the function 

# find documents that match a query

def find_Barak():
    Barak = person_collection.find_one({"first_name" : "Barak"})
    printer.pprint(Barak)
#find_Barak()  # call the function 

# count all people in our document :

def count_all_people():
    count = person_collection.count_documents(filter={})
    print("Number of people ", count)
# .count is a method that counts the number of documents in the collection.

#count_all_people()  # call the function 

# find people by id :

def get_person_by_id (person_id):
    from bson.objectid import ObjectId

    _id = ObjectId(person_id)
    person = person_collection.find_one ({"_id" : _id})
    printer.pprint(person)

#get_person_by_id("648667233c30a77a95bcdd1d") # call the function with specific given id 

# grab the people with age in a given range :
def get_age_range (min_age, max_age):
    query = {"$and" : [
              { "age": {"$gte": min_age}}, # gte = greater than and equl to 
              { "age": {"$lte": max_age}}  # lte = less than and equl to
        ]}
    
    people = person_collection.find(query).sort("age")
    for person in people:
        printer.pprint(person)

# get_age_range(36,45)

def project_columns():
    columns = { "_id": 0, "first_name": 1, "last_name": 1}
    people = person_collection.find({}, columns)
    for person in people:
        printer.pprint(person)
# putting 0 infront of column means  do not want to show in the result and putting 1 infront of the column means that we want to see it in the result 
#project_columns()


# _._._._._._._._._._._._.  Updating a Document _._._._._._._._._._._._. 

def update_person_by_id (person_id):
    from bson.objectid import ObjectId
    _id = ObjectId(person_id)

    all_updates = {
        "$set" : {"new_field" : True}, # $set is an operator to make a new field on doc with given value
        "$inc" : {"age" : 1}, # $inc is an operator to increase the value by the increament value
        "$rename" : {"first_name" : "first", "last_name" : "last"} # $rename is an operator to rename the FIELD name 
    }
#     person_collection.update_one({"_id": _id}, all_updates)

    person_collection.update_one({"_id": _id}, {"$unset" : {"new_field" : ""}}) 
    # .update_one method is used to update specific fields using various update operators 
    # $unset is an operator to DELET a field from the document


#update_person_by_id("648667233c30a77a95bcdd1c")

def replace_one(person_id):
    from bson.objectid import ObjectId
    _id = ObjectId(person_id)

    new_doc = {
        "first_name" : "Marzieh",
        "last_name" : "Mohammadi",
        "age" : 35
    }
    person_collection.replace_one({"_id" : _id}, new_doc)
      # replace is update the document with new data by keeping the same id
#replace_one("648667233c30a77a95bcdd1c")

# _._._._._._._._._._._._.  Deleting a Document _._._._._._._._._._._._. 
def delete_one_by_id (person_id):
    from bson.objectid import ObjectId
    _id = ObjectId(person_id)
    person_collection.delete_one({"_id": _id})
    #delete_one() method is used to delete a document by its unique identifier.

delete_one_by_id("648667233c30a77a95bcdd1c")

# _._._._._._._._._._._._._._._ Relationships_._._._._._._._._._._._._._._

address = {
    "_id":"648667233c30a77a95bcdd1cc56hbgg",
    "Unit number" : 5,
    "Street" : "Bay View",
    "city" : "Richmond Hill",
    "Country" : "Canada",
}

# establish a relationship between a person and an address by embedding the address document within the person document. 

def add_address_embed (person_id, address):
    from bson.objectid import ObjectId
    _id = ObjectId(person_id)

    person_collection.update_one ({"_id": _id},{"$addToSet": {'addresses': address}}) 
    # $addToSet is an operator to append the address to the addresses ARRAY field in the person document.

add_address_embed ("648667233c30a77a95bcdd1d", address)

