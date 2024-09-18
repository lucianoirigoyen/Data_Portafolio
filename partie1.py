import  requests

# Écrire une fonction get_request qui prend en paramètre une URL et utilise `requests` pour 
# effectuer une requête GET sur cette URL. Retourner dans un tuple le code de statut de la réponse 
# ainsi que le contenu en format JSON.

def get_request(url: str) -> tuple:
    r = requests.get(url)
    r.raise_for_status()
    return r.status_code , r.json() 
 
# Écrire une fonction get_countries_info prenant en paramètre une liste de code pays et une liste 
# d’informations à récupérer. En utilisant l’API REST Countries, trouver l’URL qui permet de filtrer 
# les pays via les codes pays et filtrer ceux-ci avec la liste prise en paramètre. Trouver le paramètre 
# de requête qui permet de filtrer les informations (capitale, langues, ...) retournées par l’API. 
# Retourner dans un tuple le code de statut de la réponse ainsi que le contenu en format JSON.


def get_countries_info(country_codes: list, info: list) -> tuple:
    url = "https://restcountries.com/v3.1/alpha"
    params = {}
    
    if country_codes:
        params["codes"] = ",".join(country_codes) 
    if info:
        params["fields"] = ",".join(info)  
    
    response = requests.get(url, params=params)
    return response.status_code, response.json()
# Écrire une fonction handle_request_status qui prend une URL. Faire une requête POST sur l’URL  
# Vérifier le code de statut de la réponse, si la requête est un succès, retournez le code de statut, 
# sinon trouver la fonction de `requests` qui permet de lever une exception et retourner celle-ci 
# sous forme de string à l’aide d’un bloc `try-except`.

def handle_request_status(url:str)->int|str :
    try:
        r = requests.post(url)
        r.raise_for_status()
        return r.status_code
    except requests.exceptions.HTTPError as e:
        return str(e)
# Écrire une fonction send_query_parameters qui prend un dictionnaire de paramètres de requête 
# (query parameters). Faire une requête GET sur l’URL https://httpbin.org/response-headers en 
# passant le dictionnaire en paramètres de requête. Retourner les headers de la réponse sous 
# forme de `dict`. 
 
# Écrire une fonction sent_headers qui prend un dictionnaire de headers. Faire une requête GET 
# sur l’URL https://httpbin.org/headers en y ajoutant les headers pris en paramètre. Retourner le 
# contenu dans la clé `headers` de la réponse.
    

#exercice4
def send_query_parameters(params: dict) -> dict:
    url = "https://httpbin.org/response-headers"
    r = requests.get(url, params=params)
    r.raise_for_status()
    return r.headers

def send_headers(headers: dict) -> str:
    url = "https://httpbin.org/headers"
    
    if "User-Agent" not in headers:
        headers["User-Agent"] = "Python Requests"
    
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    
    return response.json()["headers"]


   

if __name__ == "__main__":
    # Exercice 1
    url = "https://restcountries.com/v3.1/all"
    json_response, status_code = get_request(url)
    print(f"Status code (ex1): {status_code}")

    # Exercice 2
    country_codes = ["FR", "ES", "IT"]
    info = ["capital", "languages", "population"]
    status_code, json_response = get_countries_info(country_codes, info)
    print(f"Status code (ex2): {status_code}")
    print("Response JSON (ex2):", json_response)

    # Exercice 3
    print(f"Status code (ex3): {handle_request_status('https://httpbin.org/status/200')}")

    # Exercice 4
    params = {"key": "value"}
    print(f"Response headers (ex4): {send_query_parameters(params)}")

    headers = {"User-Agent": "datapool_day2"}
    print(f"Sent headers (ex4): {send_headers(headers)}")
