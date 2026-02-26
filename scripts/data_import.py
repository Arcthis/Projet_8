from pymongo import MongoClient
import json


def import_json_to_mongo(json_file, db_name, collection_name, uri="mongodb://root:a1B2c3D4e5@172.35.0.8:27017/", skip_first=False):
    client = MongoClient(uri)
    db = client[db_name]
    collection = db[collection_name]

    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    if isinstance(data, list):
        if skip_first:
            data = data[1:]
        collection.insert_many(data)
        print(f"{len(data)} documents insérés dans '{db_name}.{collection_name}'{' (premier objet ignoré)' if skip_first else ''}")
    else:
        collection.insert_one(data)
        print(f"1 document inséré dans '{db_name}.{collection_name}'")

    client.close()

if __name__ == "__main__":
    import_json_to_mongo(
        json_file="../json/merged_weather_data.json",
        db_name="backup",
        collection_name="weather_informations",
    )
    import_json_to_mongo(
        json_file="../json/stations_info.json",
        db_name="backup",
        collection_name="weather_stations"
    )

