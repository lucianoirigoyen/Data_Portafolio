import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import re
from dateutil import parser
import numpy as np
# Partie 3 : Nettoyage de données avancé 
 
# Pour les exercices 1 et 2, un fichier CSV nommé « sales_unclean.csv » vous est fourni. Pour les 
# exercices 3 et 4, un fichier CSV nommé « sales_outliers.csv » vous est fourni. Les DataFrames qui 
# vous seront passés seront tous, sauf indication contraire, chargés depuis ces fichiers pour les 
# exercices associés. À vous d’analyser ceux-ci et de comprendre ce qui vous est demandé. 


# Écrire une fonction impute_region prenant un DataFrame. Imputer les données manquantes de 
# région grâce aux informations de pays. Si pour une région manquante, le pays est également 
# manquant, alors vous ne pouvez pas trouver la région associée et vous devez donc supprimer la 
# ligne. Réinitialiser les index puis retourner le DataFrame résultant. 
 
# Indice : Analysez le dataset pour savoir quelle région correspond à quel ensemble de pays. Par 
# exemple, la région « Japan » n’est pas uniquement associée au pays « Japan ». 
 
# Écrire une fonction impute_quantity prenant un DataFrame. Compléter les données 
# manquantes de quantité par la moyenne de la colonne. Retourner le DataFrame résultant. 
 
# Data Pool Digi 2 12 
 
# Écrire une fonction impute_category prenant un DataFrame. Compléter les données 
# manquantes de catégorie par la catégorie la plus fréquente. Retourner le DataFrame résultant. 
#exercice1
def impute_region(df):
    country_to_region = {
        'USA': 'NA',
        'Canada': 'NA',
        'France': 'EMEA',
        'Norway': 'EMEA',
        'Spain': 'EMEA',
        'Italy': 'EMEA',
        'Finland': 'EMEA',
        'Austria': 'EMEA',
        'UK': 'EMEA',
        'Sweden': 'EMEA',
        'Denmark': 'EMEA',
        'Belgium': 'EMEA',
        'Germany': 'EMEA',
        'Switzerland': 'EMEA',
        'Ireland': 'EMEA',
        'Japan': 'Japan',
        'Singapore': 'Japan',
        'Philippines': 'Japan',
        'Australia':'APAC',
    }

    df['region'] = df.apply(
        lambda row: country_to_region.get(row['country']) if pd.isna(row['region']) else row['region'], axis=1)

    df = df.dropna(subset=['region', 'country'], how='all')
    df = df.reset_index(drop=True)

    return df



# Écrire une fonction impute_quantity prenant un DataFrame. Compléter les données 
# manquantes de quantité par la moyenne de la colonne. Retourner le DataFrame résultant. 
 
def impute_quantity(df):
    mean_quantity = df['quantity'].mean()
    df['quantity'] = df['quantity'].fillna(mean_quantity)

    return df

# Écrire une fonction impute_category prenant un DataFrame. Compléter les données 
# manquantes de catégorie par la catégorie la plus fréquente. Retourner le DataFrame résultant.

def impute_category(df):
    most_frequent_category = df['category'].mode()[0]
    df['category'] = df['category'].fillna(most_frequent_category)

    return df

#exercice2
 
#Écrire une fonction handle_inconsistent_dealsize qui prend en paramètre un DataFrame. 
# Convertir toutes les tailles dans le format « S / M / L ». Les formats valides sont les 
# suivants (case-insensitive) : 
# • S -> « s », « Small », « 1 » 
# • M -> « m », « Medium », « 2 » 
# • L -> « l », « Large », « 3 » 
# Si le format n’est pas reconnu, assigner M par défaut. Retourner le DataFrame résultant.


def handle_inconsistent_dealsize(df: pd.DataFrame) -> pd.DataFrame:
    df = df.columns.str.lower()
    if 'deal_size' not in df.columns  :
        df["deal_size"] = "M"
    else:
        df['deal_size'] = df['deal_size'].str.lower().replace({
            's': 'S',
            'small': 'S',
            '1': 'S',
            'm': 'M',
            'medium': 'M',
            '2': 'M',
            'l': 'L',
            'large': 'L',
            '3': 'L'
        }, regex=True)
        df['deal_size'] = df['deal_size'].fillna('M')
    
    return df
    

# crire une fonction handle_inconsistent_dates qui prend en paramètre un DataFrame. Convertir 
# toutes les dates dans le format « 2024-12-24 23:42:00 ». Il peut y avoir plusieurs types de 
# formats différents dans le DataFrame, analysez celui-ci pour trouver tous les formats existants 
# afin de les convertir proprement. En cas de doute sur le format entre dd/mm/yyyy et 
# mm/dd/yyyy, privilégier le format avec le jour en premier. Convertir toutes les valeurs de la 
# colonne « date » en type « datetime » puis retourner le DataFrame résultant
    

def identify_date_format(date_str: str) -> str:
    iso_format = r'^\d{4}-\d{2}-\d{2}( \d{2}:\d{2}:\d{2})?$'  # e.g., 2024-12-24 23:42:00
    european_format = r'^\d{2}/\d{2}/\d{4}( \d{2}:\d{2}:\d{2})?$'  # e.g., 24/12/2024 23:42:00
    us_format = r'^\d{2}/\d{2}/\d{4}( \d{2}:\d{2}:\d{2})?$'  # e.g., 12/24/2024 23:42:00 (assuming US format)

    if re.match(iso_format, date_str):
        return 'iso'
    elif re.match(european_format, date_str):
        return 'european'
    elif re.match(us_format, date_str):
        return 'us'
    else:
        return 'unknown'

def convert_date(date_str: str) -> str:
    date_format = identify_date_format(date_str)
    
    if date_format == 'iso':
        date = pd.to_datetime(date_str, format='%Y-%m-%d %H:%M:%S', errors='coerce')
    elif date_format == 'european':
        date = pd.to_datetime(date_str, format='%d/%m/%Y %H:%M:%S', errors='coerce')
    elif date_format == 'us':
        date = pd.to_datetime(date_str, format='%m/%d/%Y %H:%M:%S', errors='coerce')
    else:
        return ''
    
    if pd.notna(date):
        return date.strftime('%Y-%m-%d %H:%M:%S')
    else:
        return ''

def handle_inconsistent_dates(df: pd.DataFrame) -> pd.DataFrame:
    if 'date' in df.columns:
        df['date'] = df['date'].apply(convert_date)
        df = df[df['date'] != '']  
    return df
   
#exercice3

# Écrire une fonction retrieve_quantity_outliers qui prend un DataFrame en paramètre. Identifier 
# les quantités aberrantes avec la méthode de l’écart interquartile. On considère une valeur 
# aberrante comme une valeur étant supérieur à 1,5 fois l’IQR au-dessus du troisième quartile ou 
# en-dessous du premier quartile. Retourner un DataFrame contenant uniquement les valeurs 
# aberrantes identifiées trié par quantité croissante.


def retrieve_quantity_outliers(df: pd.DataFrame) -> pd.DataFrame:
    q1 = df['quantity'].quantile(0.25)
    q3 = df['quantity'].quantile(0.75)
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr
    outliers = df[(df['quantity'] < lower_bound) | (df['quantity'] > upper_bound)]
    outliers = outliers.sort_values(by='quantity', ascending=True)
    return outliers


def handle_unit_price_outlier(df: pd.DataFrame) -> pd.DataFrame:
   
    q1 = df['unit_price'].quantile(0.25)
    q3 = df['unit_price'].quantile(0.75)
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr
    df = df[df['unit_price'] >= lower_bound]
    df.loc[df['unit_price'] > upper_bound, 'unit_price'] = upper_bound
    
 
    df = df.sort_values(by='unit_price', ascending=True)
    




#exercice 4

 
# Écrire une fonction normalize_total_price qui prend en paramètre un DataFrame. Appliquer une 
# normalisation logarithmique sur la colonne « total_price ». Retourner le DataFrame résultant. 
# Arrondir les résultats à la dixième décimale. 




def normalize_total_price(df: pd.DataFrame) -> pd.DataFrame:
    df = df[df['total_price'] > 0].copy()
    df["total_price"] = np.log(df["total_price"])
    return df.round(10)
       

def normalize_quantity(df: pd.DataFrame) -> pd.DataFrame:
    mean = df['quantity'].mean()
    std = df['quantity'].std()
    df['quantity'] = (df['quantity'] - mean) / std
    df['quantity'] = df['quantity'].round(10)

    return df


def normalize_unit_price(df: pd.DataFrame) -> pd.DataFrame:
    min_value = df['unit_price'].min()
    max_value = df['unit_price'].max()
    df['unit_price'] = (df['unit_price'] - min_value) / (max_value - min_value)
    df['unit_price'] = df['unit_price'].round(10)

    return df


if __name__ == "__main__":
    df = pd.read_csv("/Users/lucianoleroi/Desktop/piscine/Jour6/sales_unclean.csv")
    print (impute_region(df))
    print (impute_quantity(df))
    print (impute_category(df))
    df = handle_inconsistent_dates(df)
    print(df)
    df = normalize_unit_price(df)
    outliers_df = retrieve_quantity_outliers(df)
    print(outliers_df)
    cleaned_df = handle_unit_price_outlier(df)
    print(cleaned_df)