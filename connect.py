import json
import pymongo
from bson import ObjectId
import bson.json_util

# MongoDB connection parameters
mongodb_host = 'localhost'
mongodb_port = 27017
database_name = 'mongodb_assignment_megha_TAS206'
collection_name1 = 'users'
collection_name2 = 'comments'
collection_name3 = 'theaters'
collection_name4 = 'movies'

# Connect to MongoDB
client = pymongo.MongoClient(mongodb_host, mongodb_port)
db = client[database_name]
collection1 = db[collection_name1]
collection2 = db[collection_name2]
collection3 = db[collection_name3]
collection4 = db[collection_name4]

# Read the JSON file

json_file_path1='/Users/meghasingh/MONGODB_Assignment/json files/users.json'
json_file_path2='/Users/meghasingh/MONGODB_Assignment/json files/comments.json'
json_file_path3='/Users/meghasingh/MONGODB_Assignment/json files/theaters.json'
json_file_path4='/Users/meghasingh/MONGODB_Assignment/json files/movies.json'

with open(json_file_path1, 'r') as file:
    for line in file:
        data=json.loads(line)
        bson_data = bson.json_util.loads(bson.json_util.dumps(data))
        collection1.insert_one(bson_data)

with open(json_file_path2, 'r') as file:
    for line in file:
        data=json.loads(line)
        bson_data = bson.json_util.loads(bson.json_util.dumps(data))
        collection2.insert_one(bson_data)        

with open(json_file_path3, 'r') as file:
    for line in file:
        data=json.loads(line)
        bson_data = bson.json_util.loads(bson.json_util.dumps(data))
        collection3.insert_one(bson_data)

with open(json_file_path4, 'r') as file:
    for line in file:
        data=json.loads(line)
        bson_data = bson.json_util.loads(bson.json_util.dumps(data))
        collection4.insert_one(bson_data)

print("All documents inserted!")        
# Close the MongoDB connection
client.close()