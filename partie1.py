import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Introduction aux fichiers parquet et 
# manipulation / nettoyage de données 
# avancé

#exercice1
def read_parquet(filename: str) -> pd.DataFrame:
    df_parquet = pd.read_parquet(filename)
    return df_parquet.head(10)

#exercice2
# Écrire une fonction read_parquet_columns qui charge uniquement les colonnes « columns » du 
# fichier Parquet pris en paramètre dans un DataFrame et retourne le DataFrame résultant. 

def read_parquet_columns(filename: str, columns: list[str]) -> pd.DataFrame:
    df_parquet_columns = pd.read_parquet(filename, columns=columns)

    return df_parquet_columns

#exercice3
# Écrire une fonction read_parquet_batch qui utilise la fonction iter_batches de la libarie Python 
# PyArrow afin de lire le fichier Parquet pris en paramètre par blocs de données. Utiliser le 
# paramètre de la fonction pour définir la taille de chaque bloc. 
# La fonction doit retourner dans un DataFrame les deux premières lignes de chaque bloc. 
# Attention à bien réinitialiser les index avant de retourner le DataFrame final. 
 
# Indice : Il est possible de convertir un batch PyArrow en DataFrame avec la fonction to_pandas. 

    

def read_parquet_batch(filename: str, batch_size: int) -> pd.DataFrame:
    parquet_file = pq.ParquetFile(filename)
    dataframes = []
    for batch in parquet_file.iter_batches(batch_size=batch_size):
        df_batch = batch.to_pandas()
        df_batch = df_batch.head(2)
        dataframes.append(df_batch)
    result_df = pd.concat(dataframes, ignore_index=True)
    result_df.reset_index(drop=True, inplace=True)
    return result_df



    # df_read_parquet_batch=pa.read_parquet_batch(filename, batch_size="2")










if __name__ == "__main__":
    df_parquet = read_parquet("flights.parquet")

    read_parquet()
    read_parquet_batch()
    read_parquet_columns()
    print(df_parquet)