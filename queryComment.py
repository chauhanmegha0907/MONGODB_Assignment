import pymongo
import datetime
from datetime import datetime, timedelta

# Establish MongoDB connection
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["mongodb_assignment_megha_TAS206"]

# a. Comments Collection
# Find top 10 users who made the maximum number of comments
def find_top_users():
    pipeline = [
    {"$group": {"_id": "$_id", "name": {"$addToSet": "$name"}, "count": {"$sum": 1}}},
    {"$project": {"_id": 0, "User Name": "$name", "count": 1}},
    {"$sort": {"count": -1}},
    {"$limit": 10}
]
    top_users = db.comments.aggregate(pipeline)
    return list(top_users)

# Find top 10 movies with most comments
def find_top_movies_with_most_comments():
    pipeline = [
        {"$group": {"_id": "$movie_id", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ]
    top_movies = db.comments.aggregate(pipeline)
    movie_info = []
    for movie in top_movies:
        id = movie.get("_id")
        movie_name=db.movies.find_one({"_id":id},{'title':1,'_id':0})
        count = movie.get("count")
        movie_info.append((movie_name.get('title'), count))
        

    return  movie_info

# Given a year find the total number of comments created each month in that year
from datetime import datetime

def comments_by_month(year):
    # Convert the start and end dates of the given year to Unix timestamps
    pipeline=[{"$match": {"$expr": {"$eq": [{"$year": "$date"}, year]}}},
            {"$group": {"_id": {"$month": "$date"}, "total_comments": {"$sum": 1}}},
            {"$sort": {"_id": 1}}]
    
    comment=db.comments.aggregate(pipeline)
    return comment

# Find top 10 users who made the maximum number of comments
top_users=find_top_users()
print("Top 10 users who made the maximum number of comments")
for user in top_users:
    print(user)

# # Find top 10 movies with most comments
top_movies_with_most_comments=find_top_movies_with_most_comments()
print("Top 10 movies with most comments")
for movie in top_movies_with_most_comments:
    print(movie)

print("The total number of comments created each month in that year :")
total_comments_by_month=comments_by_month(2012)
for comment_count in total_comments_by_month:
    print(comment_count)

      