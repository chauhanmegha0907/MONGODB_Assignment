import pymongo

mongodb_host = 'localhost'
mongodb_port = 27017

client = pymongo.MongoClient(mongodb_host, mongodb_port)
db = client['mongodb_assignment_megha']
collection = db['theaters']

def top_10_cities():
    pipeline = [
        {"$group": {"_id": "$location.address.city", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10}
    ]
    result = list(collection.aggregate(pipeline))
    return result


def top_10_theater_nearby(longitude, latitude):
    # Create 2dsphere index on the 'location.geo.coordinates' field  
    collection.create_index([("location.geo.coordinates", "2dsphere")])
    
    # Define the query to find theaters near the given coordinates
    query = {
        "location.geo.coordinates": {
            "$nearSphere": {
                "$geometry": {
                    "type": "Point",
                    "coordinates": [longitude, latitude]
                }
            }
        }
    }

    # Execute the query to find theaters nearby
    result = collection.find(query).limit(10)

    return list(result)

top_cities = top_10_cities()
top_nearby_theater=top_10_theater_nearby(10,20)

print("Top 10 cities with maximum number of theaters:")
for index, city_data in enumerate(top_cities, start=1):
    city_name = city_data['_id']
    theater_count = city_data['count']
    print(f"{index}. {city_name}: {theater_count} theaters")


print("Top 10 theaters nearby the given coordinates:")
for index, theater in enumerate(top_nearby_theater, start=1):
    print(f"{index}. Theater ID : {theater['theaterId']['$numberInt']} - {theater['location']['address']['street1']}, {theater['location']['address']['city']}, {theater['location']['address']['state']}")
