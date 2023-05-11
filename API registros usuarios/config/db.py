from pymongo import MongoClient

conn = MongoClient("mongodb+srv://crisszamudio:SoyHenry2023@cluster0.rs3qhkd.mongodb.net/")
db = conn['Registros_sismic_alert']  
collection = db['Users']