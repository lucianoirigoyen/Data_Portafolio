from bs4 import BeautifulSoup
import requests
import pandas as pd
from bs4 import BeautifulSoup

#exercice0
def create_bs_obj(file: str) -> BeautifulSoup :
    with open (file,"r") as f :
        html_content = f.read()
    soupe_obj = BeautifulSoup(html_content, "html.parser")
    return soupe_obj
# Écrire une fonction create_bs_obj prenant en paramètre un nom de fichier HTML. Charger ce 
# fichier, trouver et retourner le titre (balise `title`) de la page. Vous devez convertir le retour en 
# string.
#exercice 1
def create_bs_obj(file:str)->BeautifulSoup:
    with open (file,"r") as f :
        html_content = f.read()
    soupe_obj = BeautifulSoup(html_content, "html.parser")
    return soupe_obj

def find_title(file:str)-> str:
    bs_obj = create_bs_obj(file)
    title = str(bs_obj.title)
    return title

# Écrire une fonction find_paragraphs prenant en paramètre un nom de fichier HTML. Charger ce 
# fichier, trouver et récupérer toutes les balises de paragraphe `<p>`. Convertir toutes les balises en 
# string et les retourner dans une liste. 

#exercice 2


def find_paragraphs(file_name: str) -> list[str]:
    with open(file_name, "r") as file:
        html_content = file.read()
    soup = BeautifulSoup(html_content, "html.parser")
    paragraphs = soup.find_all("p")
    return [str(p) for p in paragraphs]



# Écrire une fonction find_links prenant en paramètre un nom de fichier HTML. Charger ce fichier, 
# trouver et retourner dans une liste tous les liens contenus dans les balises `<a>`. 
#exercice 3

def find_links(file: str) -> list[str]:
    with open(file, "r") as file:
        html_content = file.read()
    soup = BeautifulSoup(html_content, "html.parser")
    links = soup.find_all("a")
    return [link.get("href") for link in links]
# Écrire une fonction find_elements_with_css_class prenant en paramètre un nom de fichier HTML 
# et un nom de classe CSS. Charger le fichier, trouver et retourner dans une liste tous les 
# éléments/balises possédant la classe CSS prise en paramètre. Convertir toutes les balises en string 
# et les retourner dans une liste. 
#exercice 4
def find_elements_with_css_class(file: str, css_class: str) -> list[str]:
    with open(file, "r") as file:
        html_content = file.read()
    soup = BeautifulSoup(html_content, "html.parser")
    elements = soup.find_all(class_=css_class)
    return list([str(element) for element in elements])

# Écrire une fonction find_headers prenant en paramètre un nom de fichier HTML. Charger ce 
# fichier, extraire et retourner une liste contenant tous les textes contenu dans les éléments d'en-
# tête (`<h1>`, `<h2>`, etc.). 


#exercice 5
def find_headers(file: str) -> list[str]:
    with open(file, "r") as file:
        html_content = file.read()
    soup = BeautifulSoup(html_content, "html.parser")
    headers = soup.find_all(["h1", "h2", "h3", "h4", "h5", "h6"])
    return list(header.text for header in headers)

#exercice 6
# Écrire une fonction extract_table qui prenant en paramètre un nom de fichier HTML. Dans le 
# fichier HTML se trouvera un seul et unique tableau HTML contenant des informations sur des 
# fruits. Ce tableau possèdera 3 colonnes : ` name`, `color` et `price`. 
# Charger le fichier, analyser le tableau HTML et extraire les informations dans une liste de 
# dictionnaires, où chaque dictionnaire représente un fruit. Faire en sorte que le prix soit de type 
# `float` dans la structure de retour.

def extract_table(file: str) -> list[dict]:
    fruits = []
    with open(file, "r") as f:
        soup = BeautifulSoup(f, "html.parser")
        table = soup.find("table")
        headers = [header.text.strip().lower() for header in table.find_all("th")]
        rows = table.find_all("tr")[1:] 
        for row in rows:
            cols = row.find_all("td")
            fruit_dict = {}
            for i, col in enumerate(cols):
                header = headers[i]
                if header == 'vegetable':
                    fruit_dict['name'] = col.text.strip()
                elif header == 'color':
                    fruit_dict['color'] = col.text.strip()
                elif header == 'price':
                    fruit_dict['price'] = float(col.text.strip().replace("$", ""))
            fruits.append(fruit_dict)
    
    return fruits


if __name__ == "__main__":

    
    #exercice 1
    file = 'resources/example.html'
    title = find_title(file)
    print(title)
    #exercice 2
    paragraphs = find_paragraphs("resources/example.html")
    print(paragraphs)
    #exercice3 
    links = find_links(file)
    print(links)
    #exercice 4
    css_classes = find_elements_with_css_class(file, 'info')
    for class_elem in css_classes:
        print(class_elem)
    #exercice 5
    file = 'resources/example.html'
    headers = find_headers(file)
    print(headers) 
    #exercice 6
    file = 'resources/example.html'
    fruits = extract_table(file)
    for fruit in fruits:
        print(fruit)