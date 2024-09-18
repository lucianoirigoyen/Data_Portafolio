import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Écrire une fonction create_multi_index_df qui prend un DataFrame et l’utilise pour créer un 
# nouveau DataFrame multi-indexé par « year » puis par « region ». 
 
# Afin que le DataFrame soit indexé et découpé efficacement, il doit être trié par ses index. Deux 
# méthodes sont possibles, trier le DataFrame avant de créer les nouveaux index, ou trier 
# directement les index.

#exercice1
def create_multi_index_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.set_index(["year", "region"])
    df = df.sort_index()

    return df

# Écrire une fonction retrieve_multi_index_data qui prend un DataFrame issu de l’exercice 
# précédent. Retourner un DataFrame contenant uniquement les données pour l’année et la 
# région prisent en paramètre.
#exercice2
def retrieve_multi_index_data(df, year, region):
    filtered_df = df[(df.index.get_level_values('year') == year) &
                     (df.index.get_level_values('region') == region)]
    if not filtered_df.empty:
        return filtered_df

#exercice3
# Écrire une fonction multi_index_aggregate qui prend un DataFrame et calcule le nombre total 
# de produit vendu et le total des ventes pour chaque année et chaque région. Arrondir les 
# résultats à la deuxième décimale.
def multi_index_aggregate(df: pd.DataFrame) -> pd.DataFrame:
    df = df.groupby(["year", "region"]).agg({
        "quantity": "sum",
        "total_price": "sum",

    })
    df_agg = df_agg.map(lambda x: round(x,2) )

    return df_agg

#exercice4 
# Écrire une fonction columns_multi_index qui prend un DataFrame et reproduit les calculs 
# d’agrégation de l’exercice précédent, en ajoutant la catégorie en troisième attribut d’agrégation. 
# Vous devez ensuite pivoter l’index catégorie pour créer des colonnes Multi-Indexé. Arrondir les 
# résultats à la deuxième décimale.

def columns_multi_index(df: pd.DataFrame) -> pd.DataFrame:
     df_agg = df.groupby(["year", "region","category"]).agg({"quantity":"sum" ,"total_price":"sum"})
     df_agg = df_agg.map(lambda x: round(x,2) )
     df_pivot = df_agg.unstack(level = "category")

     return df_pivot

 
 
# Écrire une fonction swap_columns_multi_index qui prends un DataFrame issu de l’exercice 
# précédent et inverse les colonnes de catégories avec les colonnes « quantity » et « total_price » 
# de telle sorte que chaque « category » ait en sous-colonne « quantity » et « total_price ». 
# Retourner le DataFrame résultant. 

#exercice5
def swap_columns_multi_index(df:pd.DataFrame)-> pd.DataFrame:
    df_pivot = df.swaplevel(axis=1)
    return df_pivot.sort_index(axis=1)

# Écrire une fonction retrieve_multi_index_columns qui prends un DataFrame issu de l’exercice 
# précédent. Retourner un DataFrame contenant uniquement les données pour la catégorie 
# donnée. 
 
# Écrire une fonction retrieve_multi_index_basic qui prends un DataFrame issu de l’exercice 
# précédent. Retourner un DataFrame contenant uniquement les données pour la catégorie et 
# l’année donnée. 
 
# Écrire une fonction retrieve_multi_index_advanced qui prends un DataFrame issu de l’exercice 
# précédent. Retourner un DataFrame contenant uniquement les données pour la région (toute 
# années confondues) et la sous colonne donnée (« quantity » ou « total_price »).
#exercice6
def retrieve_multi_index_column(df: pd.DataFrame, category: str) -> pd.DataFrame:
    df_category = df.xs(category, axis=1, level="category") 
    return df_category

def retrieve_multi_index_basic(df: pd.DataFrame , year:int) -> pd.DataFrame :
    df_year = df[df.index.get_level_values("year")==year]
    return df_year

#exercice7
 
# Écrire une fonction create_pivot_table_basic prenant en paramètre un DataFrame. En utilisant 
# la fonction pivot_table, reproduire le DataFrame de l’exercice 4, partie 2. Arrondir les résultats à 
# la deuxième décimale et retourner le DataFrame résultant. 


def create_pivot_table_basic(df: pd.DataFrame) -> pd.DataFrame:
    df_pivot = pd.pivot_table(df, index=["year", "region"], columns="category", values=["quantity", "total_price"], aggfunc="sum")
    df_pivot = df_pivot.unstack(level="category")
    df_pivot = df_pivot.map(lambda x: round(x, 2))
    return df_pivot

def create_pivot_table_advanced(df: pd.DataFrame) -> pd.DataFrame:
    df_pivot = pd.pivot_table(df, index=["year", "region"], columns="category", values=["quantity", "total_price", "sales"], aggfunc=["sum", "mean"])
    df_pivot = df_pivot.unstack(level="category")
    df_pivot = df_pivot.map(lambda x: round(x, 2))
    return df_pivot






if __name__ == "__main__":
    df= pd.read_csv("sales.csv" ,keep_default_na=False, na_values='') 
    micdf = columns_multi_index(df) 
    print(micdf)
    
    print(micdf.columns)  
    swapdf = swap_columns_multi_index(micdf)
    category="Planes"
    df_category = retrieve_multi_index_column(swapdf, category)
    print(df_category)
    year = 2002
    df_year = retrieve_multi_index_basic(swapdf, year)
    print(df_year)
 
    pivot_df = create_pivot_table_basic(df)
    print(pivot_df)


