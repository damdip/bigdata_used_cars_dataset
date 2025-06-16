
import pandas as pd


# Legge il CSV
df = pd.read_csv('estratto_1m_dapulire.csv')


# Specifica le colonne da mantenere
colonne_da_tenere = [  'model_name','make_name' , 'year', 'price',]

# Mantieni solo quelle colonne
df = df[colonne_da_tenere]

#Rimuovi le righe con valori nulli
df_pulito = df.dropna(subset=['model_name','make_name' , 'year', 'price'])

#Salva il file modificato
df_pulito.to_csv('./dataset/dataset_job_2/input_mr_job_2_1M.csv', index=False)