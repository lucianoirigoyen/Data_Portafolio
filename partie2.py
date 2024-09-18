import pymongo
from pymongo import MongoClient
from typing import List, Dict


def get_mongo_client (host:str, port:int) -> pymongo.MongoClient:
    return pymongo.MongoClient(host, port)

#exercice1

# Écrire une fonction create_award_year_index qui crée un Index sur le champs `prizes.year` de la 
# collection `laureates`. L’Index doit être trié dans l’ordre décroissant. Retourner le nom de l’Index 
# créé
def create_award_year_index(client: MongoClient) :
    db=client.nobel
    collection=db.laureates
    resultat = collection.create_index([("prizes.year",-1)])
    return resultat

# Écrire une fonction get_laureates_year qui récupère tous les lauréats de prix Nobel pour une 
# année donné. Retourner le résultat sous forme de liste de dictionnaire
def get_laureates_year(client: MongoClient, year: int) -> list[dict] :
    db=client.nobel
    collection=db.laureates
    resultat=collection.find({"prizes.year":year})
    return list(resultat)


#exercice2

# ecrire une fonction create_country_index qui crée un Text Index sur les champs `bornCountry` et 
# `diedCountry` de la collection `laureates`. Retourner le nom de l’Index créé. 
 
# Écrire une fonction get_country_laureates qui récupère dans la base de données tous les noms, 
# prénoms, pays de naissance et pays de mort des lauréats de prix Nobel étant nés ou morts dans 
# un pays donné (par ex. « France »). Stocker et retourner le résultat dans une liste de dictionnaire
def create_country_index(client: MongoClient) :
    db=client.nobel
    collection=db.laureates
    resultat = collection.create_index([("bornCountry","text"),("diedCountry","text")])
    return resultat

def get_country_laureates(client: MongoClient, country: str) -> List[Dict]:
    db = client.nobel
    collection = db.laureates
    text_search = {"$text": {"$search": country}}
    projection = {"firstname": 1, "surname": 1, "bornCountry": 1, "diedCountry": 1, "_id": 0}
    resultats = collection.find(text_search, projection)
    return list(resultats)


#exercice3

# Écrire une fonction create_gender_category_index qui crée un Index composé sur les champs 
# `prizes.category` (ordre décroissant) et `gender` (ordre croissant) de la collection `laureates`. 
# Retourner le nom de l’Index créé. 
 
# Écrire une fonction get_gender_category_laureates qui récupère dans la base de données tous 
# les lauréats qui ont reçu un prix Nobel dans la catégorie donnée et étant du genre pris en 
# paramètre. Stocker et retourner le résultat dans une liste de dictionnaire.
def create_gender_category_index(client: MongoClient) :
    db=client.nobel
    collection=db.laureates
    resultat = collection.create_index([("prizes.category", -1), ("gender", 1)])
    return resultat

def get_gender_category_laureates(client: MongoClient, gender: str, category: str) -> list[dict]:
    db = client.nobel
    collection = db.laureates
    resultat = collection.find({"gender": gender, "prizes.category": category})
    return list(resultat)


#exercice4

# Écrire une fonction create_year_category_index qui crée un Index composé unique sur les 
# champs `year` et `category` de la collection `prizes`. Retourner le nom de l’Index créé.
def create_year_category_index(client: MongoClient):
    db = client.nobel
    collection = db.prizes
    collection.delete_many({"$or": [{"year": None}, {"category": None}]})
    index_name = collection.create_index([("year", 1), ("category", 1)], unique=True)
    
    return index_name






if __name__ == "__main__":
    #exercice1
    client=get_mongo_client('localhost', 27017)
    create_award_year_index(client)
    print (create_award_year_index(client))
    #exercice2
    client=get_mongo_client('localhost', 27017)
    create_country_index(client)
    print(create_country_index(client))
    #exercice3
    client=get_mongo_client('localhost', 27017)
    create_gender_category_index(client)
    print(create_gender_category_index(client))

    #exercice4
    client=get_mongo_client('localhost', 27017)
    create_year_category_index(client)
    print(create_year_category_index(client))

print(get_laureates_year(get_mongo_client('localhost', 27017), 2019))
 