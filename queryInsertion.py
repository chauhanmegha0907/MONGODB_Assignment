import pymongo
from datetime import datetime

# Rest of your code

# Manually enter user data
user_data = {
    "_id": "59b99db4cfa9a34dzz7885b6",
    "name": "Megha Singh",
    "email": "chauhanmegha0907@gmail.com",
    "password": "$2b$12$UREFwsRUoyF0CRqGNK0LzO0HM/jLhgUCNNIJ9RJAqMUQ74crlJ1Vu"
}

theater_data = {
    "_id": "59a47286cfa9a3a54e51v72c",
    "theaterId": 10000,
    "location": {
        "address": {
            "street1": "777 W Street",
            "city": "Bloomington",
            "state": "MN",
            "zipcode": "55425"
        },
        "geo": {
            "type": "Point",
            "coordinates": [-93.24565, 44.85466]
        }
    }
}


movies_data = {
    "_id": "573a1390f29313caazzd4135",
    "plot": "Three men hammer on an anvil and pass a bottle of beer around.",
    "genres": ["Short"],
    "runtime": 1,
    "cast": ["Charles Kayser", "John Ott"],
    "num_mflix_comments": 1,
    "title": "Blacksmith Scene",
    "fullplot": "A stationary camera looks at a large anvil with a blacksmith behind it…",
    "countries": ["USA"],
    "released": datetime(1893, 5, 9),
    "directors": ["William K.L. Dickson"],
    "rated": "UNRATED",
    "awards": {
        "wins": 1,
        "nominations": 0,
        "text": "1 win.",
        "lastupdated": "2015-08-26 00:03:50.133000000"
    },
    "year": 1893,
    "imdb": {
        "rating": 6.2,
        "votes": 1189,
        "id": 5,
        "type": "movie"
    },
    "tomatoes": {
        # Add the "viewer" object here
        "viewer": {
            "lastUpdated": datetime(2015, 6, 28, 18, 34, 9)
        }
    }
}


comments_data = {
    "_id": "573a1390f29313caazzd4135",
    "name": "Andrea Le",
    "email": "andrea_le@fakegmail.com",
    "movie_id": "573a1390f29313caabcd418c",
    "text": "Rem officiis eaque repellendus amet eos doloribus. Porro dolor volupta…",
    "date": datetime(2012, 3, 26, 23, 20, 16)
}

# Establish MongoDB connection
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["mongodb_assignment_megha_TAS208"]

# Define functions to insert data into MongoDB collections
def insert_new_comment(comments_data):
    db.comments.insert_one(comments_data)

def insert_new_movie(movies_data):
    db.movies.insert_one(movies_data)

def insert_new_theater(theater_data):
    db.theaters.insert_one(theater_data)

def insert_new_user(user_data):
    db.users.insert_one(user_data)


# Call the functions to insert data
insert_new_comment(comments_data)
insert_new_movie(movies_data)
insert_new_theater(theater_data)
insert_new_user(user_data)