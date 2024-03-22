import pymongo

# Establish MongoDB connection
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["mongodb_assignment_megha_TAS206"]


def find_top_rated_movies(N):
    pipeline = [
            {"$match": {"imdb.rating": {"$ne": ""}}},
            {"$sort": {"imdb.rating": -1}},
            {"$limit": N},
            {"$project": {"_id": 0, "title": 1, "rating": "$imdb.rating"}}
        ]

    movies=db.movies.aggregate(pipeline)
    return movies
     
def top_movies_in_year(N, year):
    pipeline = [
        {"$match": {"year": year,"imdb.rating":{"$ne":""}}},
        {"$project": {"title": 1, "imdb.rating": 1, "_id": 0}},
        {"$sort": {"imdb.rating": -1}},
        {"$limit": N}
    ]
    top_movies = db.movies.aggregate(pipeline)
    return  top_movies


# 3. Find movies with the highest IMDB rating and number of votes > 1000
def top_rated_movies_with_votes(N,votes_threshold):
    pipeline = [
            {"$match": {"imdb.rating": {"$ne": ""}, "imdb.votes": {"$gt": 1000}}},
            {"$sort": {"imdb.rating": -1}},
            {"$limit": N},
            {"$project": {"_id": 0, "title": 1, "imdb.rating": 1, "imdb.votes": 1}}
        ]
    top_rated_movie = db.movies.aggregate(pipeline)
    return  top_rated_movie

# 4. Find movies with titles matching a given pattern sorted by highest tomatoes ratings
def movies_with_title_pattern(N,title_pattern):
    pipeline = [
            {"$match": {"title": {"$regex": title_pattern, "$options": "i"}, "tomatoes.viewer.rating": {"$ne": ""}}},
            {"$sort": {"tomatoes.viewer.rating": -1}},
            {"$limit": N},
            {"$project": {"_id": 0, "title": 1, "rating": "$tomatoes.viewer.rating"}}
        ]
    sorted_movies = db.movies.aggregate(pipeline)
    return sorted_movies



# 1. Find top N directors who created the maximum number of movies
def top_directors(N):
    pipeline = [
            {"$unwind": "$directors"},
            {"$group": {"_id": "$directors", "total_movies": {"$sum": 1}}},
            {"$sort": {"total_movies": -1}},
            {"$limit": N},
            {"$project": {"_id": 1, "total_movies": 1}}
        ]
    top_directors = db.movies.aggregate(pipeline)
    return top_directors

# 2. Find top N directors who created the maximum number of movies in a given year
def top_directors_in_year(N, year):
    pipeline = [
            {"$match": {"year": year}},
            {"$unwind": "$directors"},
            {"$group": {"_id": "$directors", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": N}
        ]
    top_directors = db.movies.aggregate(pipeline)
    return top_directors


# 3. Find top N directors who created the maximum number of movies for a given genre
def top_directors_for_genre(N, genre):
    pipeline = [
            {"$match": {"genres": genre}},
            {"$unwind": "$directors"},
            {"$group": {"_id": "$directors", "total_movies": {"$sum": 1}}},
            {"$sort": {"total_movies": -1}},
            {"$limit": N}
        ]
    top_directors = db.movies.aggregate(pipeline)
    return top_directors


# 1. Find top N actors who starred in the maximum number of movies
def top_actors(N):
    pipeline = [
            {"$unwind": "$cast"},
            {"$group": {"_id": "$cast", "num_movies": {"$sum": 1}}},
            {"$sort": {"num_movies": -1}},
            {"$limit": N}
        ]
    top_actors = db.movies.aggregate(pipeline)
    return top_actors

# 2. Find top N actors who starred in the maximum number of movies in a given year
def top_actors_in_year(N, year):
    pipeline = [
            {"$match": {"year": year}},
            {"$unwind": "$cast"},
            {"$group": {"_id": "$cast", "total_movies": {"$sum": 1}}},
            {"$sort": {"total_movies": -1}},
            {"$limit": N}
        ]
    top_actors = db.movies.aggregate(pipeline)
    return top_actors

# 3. Find top N actors who starred in the maximum number of movies for a given genre
def top_actors_for_genre(N, genre):
    pipeline = [
            {"$match": {"genres": genre}},
            {"$unwind": "$cast"},
            {"$group": {"_id": "$cast", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": N}
        ]
    top_actors = db.movies.aggregate(pipeline)
    return list(top_actors)

# Find top N movies for each genre with the highest IMDB rating
def top_movies_for_each_genre(N):
    pipeline = [
            {"$unwind": "$genres"},
            {"$match": {"imdb.rating": {"$exists": True, "$ne": ""}}},
            {"$sort": {"genres": 1, "imdb.rating": -1}},
            {"$group": {"_id": "$genres", "top_movies": {"$push": {"title": "$title", "imdb_rating": "$imdb.rating"}}}},
            {"$project": {"_id": 0, "genre": "$_id", "top_movies": {"$slice": ["$top_movies", N]}}}
        ]
    top_movies_per_genre = db.movies.aggregate(pipeline)
    return list(top_movies_per_genre)
    
        


top_rated_movies = find_top_rated_movies(10)
print("Top 10 movies with the highest IMDB rating:")
for movie in top_rated_movies:
    print(movie)

year=2012
top_movies_in_year_list = top_movies_in_year(2,year)
print(f"Top movies in {year}:")
for movie in top_movies_in_year_list:
    print(movie)

top_voted_movie=top_rated_movies_with_votes(5,"1000")
print("Top movies with highest IMDB rating with number of votes > 1000 ")
for movie in top_voted_movie:
    print(movie)

top_tomatoes_movie_matching_pattern=movies_with_title_pattern(5,"la")
print("Movies with titles matching a given pattern sorted by highest tomatoes ratings")
for movie in top_tomatoes_movie_matching_pattern:
    print(movie)


top_directors_list = top_directors(5)
print("Top directors with maximum number of movies :")
for director in top_directors_list:
    print(director)

top_directors_in_year_list=top_directors_in_year(5,2015)
print("Top directors in a year:")
for director in top_directors_in_year_list:
    print(director)



genre="Short"
top_directors_for_genre_list=top_directors_for_genre(5,genre)
print(f"Top director for {genre} genre: ")
for director in top_directors_for_genre_list:
    print(director)



top_actors_list = top_actors(3)
print("Top actors:")
for actor in top_actors_list:
    print(actor)

top_actors_in_year_list = top_actors_in_year(3,"1915")
print("Top actors in a given year :")
for actor in top_actors_in_year_list:
    print(actor)

top_actors_per_genre_list = top_actors_for_genre(3,"Short")
print("Top actors in a given genre :")
for actor in top_actors_per_genre_list:
    print(actor)


movie_list=top_movies_for_each_genre(3)
print("Top movies in each genre:\n")
for i in movie_list:
    print(i,"\n")