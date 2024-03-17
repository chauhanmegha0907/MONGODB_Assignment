import json
import pymongo

# MongoDB connection parameters
mongodb_host = 'localhost'
mongodb_port = 27017
database_name = 'mongodb_assignment_megha'
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

json_file_path1='/Users/meghasingh/Desktop/mongodb assignment/new_users.json'
json_file_path2='/Users/meghasingh/Desktop/mongodb assignment/new_comments.json'
json_file_path3='/Users/meghasingh/Desktop/mongodb assignment/new_theaters.json'
json_file_path4='/Users/meghasingh/Desktop/mongodb assignment/new_movies.json'

with open(json_file_path1, 'r') as file:
    json_data = json.load(file)

# Insert the data into the collection
    

for x in json_data:
    collection1.insert_one(x)

with open(json_file_path2, 'r') as file:
    json_data = json.load(file)

# Insert the data into the collection
    

for x in json_data:
    collection2.insert_one(x)

with open(json_file_path3, 'r') as file:
    json_data = json.load(file)

# Insert the data into the collection
    

for x in json_data:
    collection3.insert_one(x)   


with open(json_file_path4, 'r') as file:
    json_data = json.load(file)

# Insert the data into the collection
    

for x in json_data:
    collection4.insert_one(x)



# Close the MongoDB connection
client.close()
