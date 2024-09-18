
from pymongo import MongoClient
import bson as bjson
from bson import ObjectId
from datetime import datetime
# Partie  4 :  Opérations  de  création,  mise  à  jour  et 
# suppression


#exercice 1
# Écrire  une  fonction  add_laureate  prenant  en  paramètre  un  dictionnaire  représentant  les 
# information d’un lauréat de prix Nobel. Insérer celui-ci dans la collection appropriée au sein de la 
# base de données `nobel`. Retourner l’id de l’objet ajout
def add_laureate(client:MongoClient,laureate:dict)->bjson.ObjectId:
    db = client.nobel
    collection = db.laureates
    collection.find_one({"firstname": laureate["firstname"], "surname": laureate["surname"],"born": laureate["born"],"bornCountry": laureate["bornCountry"],"bornCity": laureate["bornCity"],"died": laureate["died"],"diedCountry": laureate["diedCountry"],"gender": laureate["gender"]}, {"firstname": 1, "surname": 1, "_id": 1, "born": 1, "bornCountry": 1, "bornCity": 1, "died": 1, "diedCountry": 1, "gender": 1})
    result = db.laureates.insert_one(laureate)
    return result.inserted_id

#exercice2
# Écrire une fonction add_prizes prenant en paramètre une liste de dictionnaires représentant les 
# information de nouveaux prix Nobel à ajouter (par ex. les prix Nobels de cette année). Insérer 
# ceux-ci dans la collection appropriée au sein de la base de données `nobel`. Retourner la liste des 
# IDs insérés dans la Collection. 

def add_prizes(client:MongoClient,prizes:list[dict])->list[ObjectId]:
    db = client.nobel
    collection = db.prizes
    existing_prizes = []
    new_prizes =[]
    for prizes in prizes:
        existing_prizes = collection.find_one({"_id":prizes["_id"],"year":prizes["year"],"category":prizes["category"],"laureates":prizes["laureates"] },{"_id": 1, "year": 1, "category": 1, "laureates": 1})
    
        if existing_prizes is None:
            new_prizes.append(prizes)
        else:
            pass

    result = db.nobel.insert_many(new_prizes)
    return result.inserted_ids
def add_prizes(client: MongoClient, prizes: list[dict]) -> list[ObjectId]:
    db = client.nobel
    collection = db.prizes 
    new_prizes = []
    for prize in prizes:
        existing_prize = collection.find_one({
            "_id": prize["_id"],
            "year": prize["year"],
            "category": prize["category"],
            "laureates": prize["laureates"]
        })
    
        if existing_prize is None:
            new_prizes.append(prize)
        else:
            pass

    result = collection.insert_many(new_prizes)
    return result.inserted_ids
# Écrire une fonction update_laureate qui met à jour les informations de mort d’un lauréat (définir 
# par l’id pris en paramètre). Retourner dans un tuple le nombre de Documents trouvés et le 
# nombre de Documents modifiés
def update_laureate(client: MongoClient, doc_id: ObjectId, dod: str, country: str, city: str) -> (int, int):
    db = client.nobel
    collection = db.laureates
    filter_query = {"_id": doc_id}
    found_document = collection.find_one(filter_query)

    if found_document:
        update_query = {"$set": {"died": dod, "diedCountry": country, "diedCity": city}}
        result = collection.update_one(filter_query, update_query)
        
        return (1, result.modified_count)
    else:
        return (0, 0)
    

#exercice4   
# Écrire une fonction upper_categories qui met en majuscule la catégorie de tous les documents 
# dans la collection `prizes`. Retourner dans un tuple le nombre de Documents trouvés et le nombre 
# de Documents modifiés 
def upper_categories(client: MongoClient) -> (int, int):
    db = client.nobel
    collection = db.prizes
    update_query = {
        "$set": {
            "category": {"$toUpper": "$category"}
        }
    }
    result = collection.update_many({}, update_query)
    return (result.matched_count, result.modified_count)


#exercice5
# Écrire une fonction delete_prize qui supprime le prix Nobel de la collection `prizes` lié à l’id pris 
# en paramètre. Retourner dans un tuple le nombre de Documents trouvés et le nombre de 
# Documents supprimés. 

def delete_prize(client: MongoClient, prize_id: ObjectId) -> (int, int):
    db = client.nobel
    collection = db.prizes
    filter_query = {"_id": prize_id}
    found_document = collection.find_one(filter_query)
    result = collection.delete_one(filter_query)
    return (1 if found_document else 0, result.deleted_count)


