import pymongo

mongodb_host = 'localhost'
mongodb_port = 27017

client = pymongo.MongoClient(mongodb_host, mongodb_port)
db = client['mongodb_assignment_megha_TAS206']
collection = db['theaters']

db.theaters.create_index([("location.geo","2dsphere")])

def top_10_cities():
    pipeline = [
            {"$group": {"_id": "$location.address.city", "total_theaters": {"$sum": 1}}},
            {"$sort": {"total_theaters": -1}},
            {"$limit": 10}
        ]
    result = list(collection.aggregate(pipeline))
    return result


def top_10_theater_nearby(coordinates):
    pipeline = [
            {"$geoNear": {
                "near": {"type": "Point", "coordinates": coordinates},
                "distanceField": "distance",  # Specify the field to store distances
                "spherical": True
            }},
            {"$project": {
                "_id": 0,
                "theaterId": 1,
                "location.address.city": 1,
                "distance": 1
            }},
            {"$limit": 10}
        ]
    
    result = list(collection.aggregate(pipeline))
    return [{"theaterId": doc["theaterId"], "city": doc["location"]["address"]["city"], "distance": doc["distance"]} for doc in result]

top_cities = top_10_cities()
for i in top_cities:
    print(i,"\n")

top_nearby_theater=top_10_theater_nearby([10,20])
for i in top_nearby_theater:
    print(i,"\n")