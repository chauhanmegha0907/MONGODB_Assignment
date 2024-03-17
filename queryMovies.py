import pymongo

# Establish MongoDB connection
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["mongodb_assignment_megha"]


def find_top_rated_movies(N):
    top_rated_movies = db.movies.find({},{"title": 1, "imdb.rating": 1, "_id": 0}).sort("imdb.rating", -1).limit(N)
    movie_info = [(movie["title"], int(movie["imdb"]["rating"]["$numberInt"])) for movie in top_rated_movies]
    return movie_info

   
def top_movies_in_year(N, year):
    pipeline = [
        {"$match": {"year.$numberInt": year }},
        {"$project": {"title": 1, "imdb.rating": 1, "_id": 0}},
        {"$sort": {"imdb.rating": -1}},
        {"$limit": N}
    ]
    top_movies = db.movies.aggregate(pipeline)
    # return top_movies
    movie_info = []
    for movie in top_movies:
        title = movie.get("title")
        rating = movie.get("imdb", {}).get("rating", {}).get("$numberDouble")
        movie_info.append((title, rating))
        

    return  movie_info


# 3. Find movies with the highest IMDB rating and number of votes > 1000
def top_rated_movies_with_votes(N,votes_threshold):
    pipeline = [
        {"$match": {"imdb.votes.$numberInt": {"$gt": votes_threshold}}},
        {"$sort": {"imdb.rating": -1}},
        {"$limit": N}
    ]
    top_rated_movie = db.movies.aggregate(pipeline)
    movie_info = []
    for movie in top_rated_movie:
        title = movie.get("title")
        rating = movie.get("imdb", {}).get("rating", {}).get("$numberInt")
        movie_info.append((title, rating))
        

    return  movie_info

# 4. Find movies with titles matching a given pattern sorted by highest tomatoes ratings
def movies_with_title_pattern(N,title_pattern):
    regex_pattern = ".*" + title_pattern + ".*"
    pipeline = [
        {"$match": {"title": {"$regex": regex_pattern, "$options": "i"}}},
        {"$sort": {"tomatoes.viewer.rating": -1}},
        {"$limit": N}
    ]
    sorted_movies = db.movies.aggregate(pipeline)
    movie_info = []
    for movie in sorted_movies:
        title = movie.get("title")
        rating = movie.get("tomatoes", {}).get("viewer", {}).get("rating").get("$numberInt")
        movie_info.append((title, rating))
        

    return  movie_info

# 1. Find top N directors who created the maximum number of movies
def top_directors(N):
    pipeline = [
        {"$group": {"_id": "$directors", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$addFields": {"name": "$_id"}},
        {"$project": { "name":1 ,"count": 1,"_id":0}},
        {"$skip":1},
        {"$limit": N}
    ]
    top_directors = db.movies.aggregate(pipeline)
    return list(top_directors)

# 2. Find top N directors who created the maximum number of movies in a given year
def top_directors_in_year(N, year):
    pipeline = [
        {"$match": {"year.$numberInt": year }},
        {"$group": {"_id": "$directors", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$addFields": {"name": "$_id"}},
        {"$project": { "count":1 ,"name": 1,"_id":0}},
        {"$limit": N}
    ]
    top_directors = db.movies.aggregate(pipeline)
    return list(top_directors)


# 3. Find top N directors who created the maximum number of movies for a given genre
def top_directors_for_genre(N, genre):
    pipeline = [
        {"$match": {"genres": genre}},
        {"$group": {"_id": "$directors", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$addFields": {"name": "$_id"}},
        {"$project": { "count":1 ,"name": 1,"_id":0}},
        {"$limit": N}
    ]
    top_directors = db.movies.aggregate(pipeline)
    return list(top_directors)


# 1. Find top N actors who starred in the maximum number of movies
def top_actors(N):
    pipeline = [
        {"$unwind": "$cast"},
        {"$group": {"_id": "$cast", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$addFields": {"Actor Name": "$_id"}},
        {"$project": { "count":1 ,"Actor Name": 1,"_id":0}},
        {"$limit": N}
    ]
    top_actors = db.movies.aggregate(pipeline)
    return list(top_actors)

# 2. Find top N actors who starred in the maximum number of movies in a given year
def top_actors_in_year(N, year):
    pipeline = [
        {"$match": {"year.$numberInt": year }},
        {"$unwind": "$cast"},
        {"$group": {"_id": "$cast", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$addFields": {"Actor Name": "$_id"}},
        {"$project": { "count":1 ,"Actor Name": 1,"_id":0}},
        {"$limit": N}
    ]
    top_actors = db.movies.aggregate(pipeline)
    return list(top_actors)

# 3. Find top N actors who starred in the maximum number of movies for a given genre
def top_actors_for_genre(N, genre):
    pipeline = [
        {"$unwind":"$genres"},
        {"$unwind": "$cast"},
        {"$match":{"genres":genre}},
        {"$group": {"_id": "$cast", "count": {"$sum": 1}}},
        {"$addFields": {"Actor Name": "$_id"}},
        {"$project": { "count":1 ,"Actor Name": 1,"_id":0}},
        {"$sort": {"count": -1}},
        {"$limit": N}
    ]
    top_actors = db.movies.aggregate(pipeline)
    return list(top_actors)

# Find top N movies for each genre with the highest IMDB rating
def top_movies_for_each_genre(N):
    pipeline = [
        {"$unwind": "$genres"},
        {"$group": {"_id": "$genres", "top_movies": {"$push": {"title": "$title", "imdb_rating": "$imdb.rating"}}}},
        {"$project": {"top_movies": {"$slice": ["$top_movies", N]}}},
    ]
    top_movies_per_genre = db.movies.aggregate(pipeline)
    # return list(top_movies_per_genre)

    genre_list = []
    movie_list=[]
    for eachGenre in top_movies_per_genre:
        genres=eachGenre.get("_id")
        top_movies=eachGenre.get("top_movies")
        genre_list.append(genres)
        genre_movie_list=[]
        for movie in top_movies:
            movie_title=movie.get('title')
            movie_rating=movie.get('imdb_rating').get('$numberDouble')
            genre_movie_list.append((movie_title,movie_rating))
        movie_list.append(genre_movie_list)

    
    return genre_list,movie_list
    
        


top_rated_movies = find_top_rated_movies(10)
print("Top 10 movies with the highest IMDB rating:")
for movie in top_rated_movies:
    print(movie)

year="1915"
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

top_directors_in_year_list=top_directors_in_year(5,"1915")
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


genre_list,movie_list=top_movies_for_each_genre(3)
for index,each_genre in enumerate(genre_list):
        print(f"Movie genre : {each_genre}")
        for movie in movie_list[index]:
            print(movie)





