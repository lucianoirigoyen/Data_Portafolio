import pymongo
from pymongo import MongoClient
from typing import List, Dict
from bson import ObjectId


# Écrire une fonction get_mongo_client permettant de se connecter à une instance de MongoDB. 
# Utiliser les paramètres `host` et `port` pour créer cette connexion. Retourner l’objet résultant.
#exercice 1
def get_mongo_client (host:str, port:int) -> pymongo.MongoClient:
    return pymongo.MongoClient(host, port)
# Écrire une fonction get_all_laureates qui récupère dans la base de données tous les lauréats de 
# prix Nobel. Stocker et retourner le résultat dans une liste de dictionnaire.

#exercice2
def get_all_laureates(client:MongoClient)->list[dict]:
    db = client.nobel
    db.laureates.find({})
    laureates = list(db.laureates.find({}))
    return laureates
    


#exercice 3
# Écrire une fonction get_laureates_information qui récupère dans la base de données tous les 
# noms, prénoms et date de naissance des lauréats de prix Nobel. Stocker et retourner le résultat 
# dans une liste de dictionnaire. 
 
# Écrire une fonction get_prize_categories qui récupère dans la base de données toutes les 
# catégories de prix Nobel existantes. Stocker et retourner le résultat dans une liste de string. 
def get_laureates_information(client: MongoClient) -> list[dict]:
    db = client.nobel
    laureats={"_id": 0,"firstname":1,"born":1,"surname":1}
    resultats = db.laureates.find({}, laureats)
    return list(resultats)

def get_prize_categories(client: MongoClient) -> list[str]:
    db = client.nobel
    db.prizes.distinct("category")
    categories = list(db.prizes.distinct("category"))
    return categories

#exercice4
# Écrire une fonction get_category_laureates qui récupère dans la collection « laureates » tous les 
# noms, prénoms et catégories des lauréats de prix Nobel dans une catégorie donnée (par ex. 
# « physics »). Stocker et retourner le résultat dans une liste de dictionnaire. 
 
# Écrire une fonction get_country_laureates qui récupère dans la collection « laureates » tous les 
# noms, prénoms et pays de naissance lauréats de prix Nobel étant né dans un pays donné (par ex. 
# « France »). Stocker et retourner le résultat dans une liste de dictionnaire

def get_category_laureates(client: MongoClient, category: str) -> List[Dict]:
    db = client.nobel
    collection = db.laureates
    # Utiliser une regex MongoDB pour trouver les documents avec la catégorie donnée
    category_filter = {"prizes.category": {"$regex": f"^{category}$", "$options": "i"}}
    # Projection pour ne récupérer que les champs nécessaires
    fields = {"firstname": 1, "surname": 1, "prizes": 1, "_id": 0}

    results = collection.find(category_filter, fields)
    formatted_results = []
    
    for result in results:
        # Filtrer les prix pour ne conserver que ceux qui correspondent à la catégorie
        prizes = [prize for prize in result.get("prizes", []) if prize.get("category", "").lower() == category.lower()]
        formatted_results.append({
            "firstname": result.get("firstname"),
            "surname": result.get("surname"),
            "prizes": [{"category": prize.get("category")} for prize in prizes]
        })
    
    return formatted_results


def get_country_laureates(client: MongoClient, country: str) -> List[Dict]:
    db = client.nobel
    collection = db.laureates
    country_filter = {"bornCountry": {"$regex": country, "$options": "i"}}
    fields = {"firstname": 1, "surname": 1, "bornCountry": 1, "_id": 0}
    resultats = collection.find(country_filter, fields)
    return list(resultats)
#exercice5
# Écrire une fonction get_shared_prizes qui récupère dans la base de données tous les prix Nobel 
# qui ont été partagé entre plusieurs personnes. Stocker et retourner le résultat dans une liste de 
# dictionnaire. 

def get_shared_prizes(client:MongoClient)->list[dict]:
    db = client.nobel
    collection = db.prizes
    category_shared = {"laureates.share": {"$gte": 2}}
    resultats = collection.find(category_shared)
     
    return list(resultats)

def get_shared_prizes_common(client: MongoClient) -> list[dict]:
    db = client.nobel
    collection = db.prizes
    
    pipeline = [
        # Décomposer les lauréats pour pouvoir les compter individuellement
        {'$unwind': {'path': '$laureates', 'preserveNullAndEmptyArrays': True}},
        
        # Regrouper par year, category et motivation
        {'$group': {
            '_id': {
                'year': '$year',
                'category': '$category',
                'motivation': '$laureates.motivation'
            },
            'count': {'$sum': 1},
            'laureates': {'$push': '$laureates'}
        }},
        
        # Filtrer pour garder seulement les prix partagés entre exactement 2 personnes
        {'$match': {'count': 2}},
        
        # Ajouter une vérification pour s'assurer que le champ 'motivation' existe dans les lauréats
        {'$project': {
            '_id': {'$toString': '$_id.year'},
            'year': '$_id.year',
            'category': '$_id.category',
            'motivation': '$_id.motivation',
            'laureates': {
                '$filter': {
                    'input': '$laureates',
                    'as': 'laureate',
                    'cond': {'$ne': ['$laureate.motivation', None]}
                }
            }
        }},
        
        # Trier les résultats par année et catégorie
        {'$sort': {'year': -1, 'category': 1}}
    ]
    
    resultats = collection.aggregate(pipeline)
    shared_prizes = list(resultats)
    
    # Ajouter ObjectId à _id pour chaque prix
    for prize in shared_prizes:
        prize['_id'] = ObjectId(prize['_id'])
    
    return shared_prizes
# Écrire une fonction get_laureates_information_sorted qui récupère les mêmes informations que 
# la fonction éponyme de l’exercice 3 ainsi que le pays de naissance et applique un tri par pays de 
# naissance dans l’ordre alphabétique inverse et par date de naissance croissante. Retourner le 
# résultat sous forme de liste de dictionnaire.
#exercice6 
def get_laureates_information_sorted(client: MongoClient) -> list[dict]:
     db = client.nobel
     db.prizes
     laureats={"_id": 0,"firstname":1,"born":1,"surname":1,"bornCountry":1}
     resultats = db.laureates.find({}, laureats).sort([("bornCountry",-1),("born",1)])
     return list(resultats)
     


if __name__ == "__main__":
     #exercice2
     client = get_mongo_client('localhost', 27017)
     laureates = get_all_laureates(client)
     print(laureates[-1])
    #exercice3 
     client = get_mongo_client('localhost', 27017)
     laureates = get_laureates_information(client)
     print(laureates)

    #exercice4
     client = get_mongo_client('localhost', 27017)
     get_category_laureates(client, "physics")
     get_country_laureates(client, "United Kingdom")

    #exercice5
     client = get_mongo_client('localhost', 27017)
     get_shared_prizes(client)
    
    #exercice6
     client = get_mongo_client('localhost', 27017)
     get_laureates_information_sorted(client)
   