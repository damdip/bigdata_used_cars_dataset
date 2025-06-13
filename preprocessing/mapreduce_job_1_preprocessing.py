import pandas as pd

def rimuovi_nulli(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rimuove tutte le righe del DataFrame che contengono valori nulli (NaN).

    Parametri:
        df (pd.DataFrame): il DataFrame di input.

    Restituisce:
        pd.DataFrame: il DataFrame senza righe con valori nulli.
    """
    df_clean = df.dropna()
    return df_clean
