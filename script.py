import pymongo
import requests
import pandas as pd
import time

# Obtener datos de la API
def get_data():
    url = "https://www.jma.go.jp/bosai/quake/data/list.json"
    response = requests.get(url)
    data = response.json()
    return data

# Función para eliminar duplicados
def eliminar_duplicados():
    # Conexión a MongoDB Atlas
    client = pymongo.MongoClient("mongodb+srv://vansik:Dcshooes_4@cluster0.yqzaoxt.mongodb.net/test?retryWrites=true&w=majority")
    db = client["SismicJpAlertjp"]
    collection = db["jp"]

    # Obtener fechas únicas
    unique_dates = collection.distinct("at")

    # Borrar documentos duplicados
    for date in unique_dates:
        collection.delete_many({"at": {"$ne": date}})

# Función para eliminar columnas
def eliminar_columnas():
    # Conexión a MongoDB Atlas
    client = pymongo.MongoClient("mongodb+srv://vansik:Dcshooes_4@cluster0.yqzaoxt.mongodb.net/test?retryWrites=true&w=majority")
    db = client["SismicJpAlertjp"]
    collection = db["jp"]

    # Eliminar columnas innecesarias
    collection.update_many({}, {"$unset": {"ttl": 1}}) 
    collection.update_many({}, {"$unset": {"ift": 1}}) 
    collection.update_many({}, {"$unset": {"ctt": 1}}) 
    collection.update_many({}, {"$unset": {"eid": 1}}) 
    collection.update_many({}, {"$unset": {"ser": 1}}) 
    collection.update_many({}, {"$unset": {"acd": 1}}) 
    collection.update_many({}, {"$unset": {"cod": 1}}) 
    collection.update_many({}, {"$unset": {"int": 1}}) 
    collection.update_many({}, {"$unset": {"json": 1}}) 
    collection.update_many({}, {"$unset": {"en_ttl": 1}}) 

# Función para insertar datos en MongoDB
def insertar_datos():
    # Obtener datos
    data = get_data()

    # Crear lista de diccionarios
    earthquake_data = []
    for item in data:
        earthquake = {
            "Observed at": item["at"],
            "Place name of epicenter": item["en_anm"],
            "Magnitude": item["mag"],
            "Maximum seismic intensity": item["maxi"],
            "Date and time of issuance": item["rdt"]
        }
        earthquake_data.append(earthquake)

    # Conexión a MongoDB Atlas
    client = pymongo.MongoClient("mongodb+srv://vansik:Dcshooes_4@cluster0.yqzaoxt.mongodb.net/test?retryWrites=true&w=majority")
    db = client["SismicJpAlertjp"]
    collection = db["jp"]

    # Insertar datos en la colección de MongoDB
    collection.insert_many(earthquake_data)

# Función principal
def main():
    while True:
        insertar_datos()
        eliminar_duplicados()
        eliminar_columnas()
        time.sleep(60) # Esperar 1 minuto

if __name__ == '__main__':
    main()
