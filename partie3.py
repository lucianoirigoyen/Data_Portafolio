from typing import List, Dict
from pymongo import MongoClient


#exercice 1
# Écrire une fonction prizes_per_category_basic qui calcule et retourne le nombre de prix Nobel 
# décernés dans chaque catégorie. Un prix Nobel décerné à plusieurs personnes est considéré 
# comme un seul prix. Retourner une liste de dictionnaire contant le résultat de l’agrégation (voir 
# exemple pour le format)
def prizes_per_category_basic(client:MongoClient) -> list[dict]:
    db = client.nobel
    collection = db.prizes
    resultat=collection .aggregate([{"$group": {"_id": "$category", "prizes": {"$sum": 1}}}])
    return list(resultat)

#exercice 2
# Écrire une fonction prizes_per_category_sorted qui calcule et retourne le nombre de prix Nobel 
# décernés dans chaque catégorie, trié par nombre de prix Nobel décroissant puis par catégorie par 
# ordre alphabetique. Un prix Nobel décerné à plusieurs personnes est considéré comme un seul 
# prix. Retourner une liste de dictionnaire contant le résultat de l’agrégation. 
 
client = MongoClient('localhost', 27017) 
result = prizes_per_category_basic(client) 
for elem in result: 
    print(elem) 
 
  

def prizes_per_category_sorted(client:MongoClient) -> list[dict]:
    db = client.nobel
    collection = db.prizes
    return collection.aggregate([{"$group": {"_id": "$category", "prizes": {"$sum": 1}}},({"$sort": {"prizes": -1 , "_id":1}})])

#exercice3
# Écrire une fonction prizes_per_category_filtered qui calcule et retourne le nombre de prix Nobel 
# décernés dans chaque catégorie. Seuls les prix Nobels décernés à `nb_laureates` doivent être pris 
# en compte. Retourner une liste de dictionnaire contant le résultat de l’agrégation.
def prizes_per_category_filtered(client:MongoClient,nb_laureates:int)->list[dict]:
    db=client.nobel
    collection=db.prizes
    pipeline = [
       
        {"$unwind": "$laureates"},
        {"$match": {"laureates.share": nb_laureates}},
        {"$group": {
            "_id": "$category",
            "prizes": {"$addToSet": "$_id"}
        }},
        {"$project": {
            "prizes": {"$size": "$prizes"}
        }},
        {"$sort": {"prizes": -1, "_id": 1}}
    ]
    
    results = list(collection.aggregate(pipeline))
    return results
#exercice4
# Écrire une fonction prizes_per_category qui calcule et retourne le nombre de prix Nobel décernés 
# dans chaque catégorie, trié par nombre de prix Nobel décroissant puis par catégorie par ordre 
# alphabetique. Seuls les prix Nobels décernés à `nb_laureates` doivent être pris en compte. 
# Retourner une liste de dictionnaire contant le résultat de l’agrégation. 
 


def prizes_per_category(client: MongoClient, nb_laureates: int) -> list[dict]:
    db = client.nobel
    collection = db.prizes
    pipeline = [ {"$match": {"laureates": {"$size": nb_laureates}}},{"$group": {"_id": "$category","count": {"$sum": 1}}},
        {"$sort": {"count": -1,"_id": 1}},
        {"$project": {"_id": 0,"category": "$_id","count": 1}} ]
    return list(collection.aggregate(pipeline))


 
 
 
client = MongoClient('localhost', 27017) 
result = prizes_per_category_filtered(client, nb_laureates=1) 
for elem in result: 
    print(elem) 
 
  

# Écrire une fonction laureates_per_birth_country_complex qui calcule et retourne le nombre de 
# lauréats par pays de naissance qui sont mortes dans leur pays de naissance, ou qui sont encore 
# vivantes. Trier le résultat par les pays de naissance dans l’ordre alphabétique. Retourner une liste 
# de dictionnaire contant le résultat de l’agrégation. 
 
# On considère qu’un lauréat est encore vivant si la valeur de l’attribut `died` vaut `0000-00-00` 

def laureates_per_birth_country_complex2(client: MongoClient) -> list[dict]:
    db=client.nobel
    collection=db.laureates
    result=collection.aggregate({
        [
  {"$match":{"died":"0000-00-00"}},
  {"$match":{"diedCountry":"$birthCountry"}},
  {"$group":{"_id":"$birth_country","count":{"$sum":1}}},
  {"$sort":{"count":1,"_id":1}}

             ] }) 
    return list(result)  






                                 
                                 
                                 




if __name__ == "__main__":

    client = MongoClient('localhost', 27017)
    result = prizes_per_category_basic(client)
    for elem in result:
       print(elem)

    result = prizes_per_category_sorted(client)
    for elem in result:
        print(elem)

    client = MongoClient('localhost', 27017)
    result = prizes_per_category_filtered(client, nb_laureates=1)
    for elem in result:
     print(elem)

    prizes_per_category(client, nb_laureates=1)

    laureates_per_birth_country_complex2(client)