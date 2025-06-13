
import pandas as pd
from preprocessing.mapreduce_job_1_preprocessing import rimuovi_nulli


# Legge il CSV
df = pd.read_csv('estratto_1m_dapulire.csv')

# Mostra le prime 5 righe
print(df.head())


# Specifica le colonne da mantenere
colonne_da_tenere = ['city', 'year', 'price', 'daysonmarket', 'description']

# Mantieni solo quelle colonne
df = df[colonne_da_tenere]

# Applica il preprocessing
df_pulito = rimuovi_nulli(df)

#Salva il file modificato
df_pulito.to_csv('./dataset/dataset_job_2/input_mr_job_2_1M.csv', index=False)

#Rileggo il dataset salvato
df_pulito = pd.read_csv('./dataset/dataset_job_2/input_mr_job_2_1M.csv')

print(df_pulito.head())




