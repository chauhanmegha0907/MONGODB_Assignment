import pymongo

new_comment = {
    "user_id": "user123",
    "movie_id": "movie456",
    "text": "This movie is great!",
    "date": "2024-03-16"
}

new_movie = {
    "title": "New Movie",
    "year": 2024,
    "genre": ["Action", "Adventure"],
    "imdb_rating": 8.5
}

new_theater = {
    "name": "New Theater",
    "city": "New York",
    "capacity": 200
}

new_user = {
    "username": "new_user",
    "email": "new_user@example.com",
    "age": 30
}

# Establish MongoDB connection
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["mongodb_assignment_megha"]

# Define functions to insert data into MongoDB collections
def insert_new_comment(comment_data):
    db.comments.insert_one(comment_data)

def insert_new_movie(movie_data):
    db.movies.insert_one(movie_data)

def insert_new_theater(theater_data):
    db.theaters.insert_one(theater_data)

def insert_new_user(user_data):
    db.users.insert_one(user_data)


# Call the functions to insert data
insert_new_comment(new_comment)
insert_new_movie(new_movie)
insert_new_theater(new_theater)
insert_new_user(new_user)






