import pymongo
import datetime
from datetime import datetime, timedelta

# Establish MongoDB connection
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["mongodb_assignment_megha"]

# a. Comments Collection
# Find top 10 users who made the maximum number of comments
def find_top_users():
    pipeline = [
        {"$group": {"_id": "$name", "count": {"$sum": 1}}},
        {"$project": {"User Name": "$_id", "count": 1,"_id":0}},
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
        id = movie.get("_id").get('$oid')
        movie_name=db.movies.find_one({"_id.oid":id},{'title':1,'_id':0})
        count = movie.get("count")
        movie_info.append((movie_name['title'], count))
        

    return  movie_info

# Given a year find the total number of comments created each month in that year
from datetime import datetime

def comments_by_month(year):
    # Convert the start and end dates of the given year to Unix timestamps
    start_of_year = datetime(year, 1, 1)
    end_of_year = datetime(year + 1, 1, 1)

    start_unix_timestamp = int(start_of_year.timestamp())
    end_unix_timestamp = int(end_of_year.timestamp())


    # Aggregation pipeline to group comments by month and year
    pipeline = [
        # Filter documents within the given year
        {"$match": {
        "date.$date.$numberLong": {"$gte": str(start_unix_timestamp), "$lt": str(end_unix_timestamp)}
        }},
        # Group comments by month and year
        {"$group": {
            "_id": {"month": "$month", "year": "$year"},
            "total_comments": {"$sum": 1}
        }},
        # Project to reshape the output
        {"$project": {
            "_id": 0,
            "month": "$_id.month",
            "year": "$_id.year",
            "total_comments": 1
        }}
    ]

    # Execute the aggregation pipeline
    result = db.comments.aggregate(pipeline)

    # Convert the result to a list
    comments_by_month = list(result)

    return comments_by_month

# Find top 10 users who made the maximum number of comments
top_users=find_top_users()
print("Top 10 users who made the maximum number of comments")
for user in top_users:
    print(user)

# Find top 10 movies with most comments
top_movies_with_most_comments=find_top_movies_with_most_comments()
print("Top 10 movies with most comments")
for movie in top_movies_with_most_comments:
    print(movie)

print("The total number of comments created each month in that year :")
total_comments_by_month=comments_by_month(2012)
for comment_count in total_comments_by_month:
    print(comment_count)

