from bs4 import BeautifulSoup
import os
import requests
import time 
from urllib.parse import urljoin

#exercice 1
# Écrire une fonction find_links_in_paragraphs qui prend une URL en paramètre. Appeler cette URL 
# avec `requests` et récupérer le contenu de la réponse si la requête est un succès, sinon lever une 
# erreur.  
# Analyser le contenu de la page pour récupérer tous les liens contenu dans un paragraphe. 
# Retourner ceux-ci dans une liste.

def find_links_in_paragraphs(url: str) -> list[str]:
    try:
        r = requests.get(url)
        r.raise_for_status()
        if r.status_code == 200:
            html_content = r.content  
            soup = BeautifulSoup(html_content, "html.parser")
            paragraphs = soup.find_all("p")  
            links = []
            for paragraph in paragraphs:
                links.extend([str(link.get("href")) for link in paragraph.find_all("a")])
            return links
    except requests.exceptions.HTTPError as e:
        print(e)

#exercice2
# Écrire une fonction download_images qui prend une URL Wikipedia en paramètre et un chemin 
# vers un dossier. Appeler cette URL avec `fetch_html`, trouver et télécharger toutes les images, 
# n’étant pas des images statiques, dans le dossier pris en paramètre. 
# La fonction prend un paramètre optionnel « max », s’il vaut « None » (valeur par défaut), 
# télécharger toutes les images, sinon arrêtez-vous à « max » images

def download_images(url: str, folder: str, max: int | None = None):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
    
    
    response = requests.get(url, headers=headers)
    response.raise_for_status()  
    soup = BeautifulSoup(response.content, 'html.parser')
    images = soup.find_all('img', src=lambda x: x and not x.startswith('/static/'))
    
    
    for i, image in enumerate(images):
        if max is not None and i >= max:
            break
        image_url = image['src']
        image_url = urljoin(url, image_url)  
        image_response = requests.get(image_url, headers=headers)
        image_response.raise_for_status()
        image_name = os.path.basename(image_url)
        with open(os.path.join(folder, image_name), 'wb') as f:
            f.write(image_response.content)
        
        time.sleep(1)

# Écrire une fonction recursive_navigation qui prend une URL du Wikipedia Français et un nombre. 
# Appeler cette URL avec `fetch_html` et récupérer le nbième lien contenu dans un paragraphe et qui 
# réfère à une autre page du Wikipedia Français. Appeler ce lien et répéter l’opération en enlevant 
# 1 à `nb` à chaque itération et ce jusqu’à ce que `nb` vaille 0. 
# Retourner une liste contenant tous les liens appelés dans l’ordre d’appel. 
#exercice3
def fetch_html(url: str) -> str:
    response = requests.get(url)
    response.raise_for_status()
    return response.content

def recursive_navigation(url: str, nb: int) -> list[str]:
    visited_urls = []

    while nb > 0:
        try:
            html_content = fetch_html(url)
            soup = BeautifulSoup(html_content, 'html.parser')
            links = [a['href'] for a in soup.find_all('p') 
                     for a in a.find_all('a', href=True) 
                     if a['href'].startswith('/wiki/') and ':' not in a['href']]
            
            if nb - 1 < len(links):
                url = "https://fr.wikipedia.org" + links[nb - 1]
                visited_urls.append(url)
                nb -= 1
            else:
                break
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            break

    return visited_urls

# Dans un navigateur web, aller sur la page d’accueil de Books to Scrape et inspecter le code HTML 
# du premier livre affiché afin de trouver dans quels éléments HTML se trouvent les informations 
# suivantes : le titre(string), la note (nombre entier, int), et le prix (nombre décimal, float). 
 
# Suite à cette analyse, écrire une fonction get_one_book qui récupère les informations du premier 
# livre. Retourner celles-ci dans un dictionnaire avec les clés suivantes : `title`, `rating` et `price`.


def get_one_book():
    url = "http://books.toscrape.com/"
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    book = soup.find('article', class_='product_pod')
    title = book.h3.a['title']
    rating_class = book.p['class'][-1]
    price = book.find('p', class_='price_color').text
    rating_map = {
        'One': 1,
        'Two': 2,
        'Three': 3,
        'Four': 4,
        'Five': 5
    }
    rating = rating_map.get(rating_class, 0)
    price = price.encode('ascii', 'ignore').decode('ascii')
    price = float(price.replace('£', '').strip())
    return {
        'title': title,
        'rating': rating,
        'price': price
    }


# En continuant l’analyse de la structure HTML du premier livre, trouver le lien vers la page détaillée 
# du livre et analyser cette nouvelle page pour trouver l’élément HTML contenant la description du 
# livre.  
 
# Écrire une fonction get_one_book_complete qui récupère les mêmes informations que dans 
# l’exercice précédent mais également la description du livre. Retourner les informations dans un 
# dictionnaire avec les clés suivantes : `title`, `rating`, `price` et `description`.
def get_one_book_complete():
    url = "http://books.toscrape.com/"

    html_content = fetch_html(url)

    soup = BeautifulSoup(html_content, 'html.parser')

    book = soup.select_one('article.product_pod')

    relative_link = book.h3.a['href']
    detail_url = requests.compat.urljoin(url, relative_link)

    detail_html_content = fetch_html(detail_url)

    detail_soup = BeautifulSoup(detail_html_content, 'html.parser')

    title = book.h3.a['title']
    rating_class = book.p['class']
    rating = rating_class[-1]
    rating_map = {
        'One': 1,
        'Two': 2,
        'Three': 3,
        'Four': 4,
        'Five': 5
    }
    rating = rating_map.get(rating, 0)
    price = book.select_one('p.price_color').text.strip().replace('£', '').replace('Â', '').replace(',', '.')
    try:
        price = float(price)
    except ValueError:
        print(f"Failed to convert price to float: {price}")
        price = None

    description_tag = detail_soup.select_one('meta[name="description"]')
    description = description_tag['content'].strip() if description_tag else 'No description available'

    return {
        'title': title,
        'rating': rating,
        'price': price,
        'description': description
    }



if __name__ == "__main__":
    #exercice 1
    url = "https://fr.wikipedia.org/wiki/Chamaeleonidae"
    links = find_links_in_paragraphs(url)
    print(links)
     # exercice 2
    url = "https://fr.wikipedia.org/wiki/Chamaeleonidae"
    folder = "images"  
    if not os.path.exists(folder):
        os.makedirs(folder)
    download_images(url, folder)