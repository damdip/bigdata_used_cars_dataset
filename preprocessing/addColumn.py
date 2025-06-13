import pandas as pd

# Supponiamo di avere due DataFrame:
df1 = pd.read_csv("./dataset/dataset_job_1/input_mr_job_1_100k.csv")

df2 = pd.read_csv("./dataset/dataset_job_2/dataset_job2_100k.csv")

print(df1.head())
print(df2.head())



# Supponiamo che i tuoi DataFrame siano già caricati come df1 e df2

# 1. Estrai la colonna 'model_name' da df1 (allineamento per indice)
model_col = df1['model_name'].reset_index(drop=True)

# 2. Inserisci 'model_name' in df2 subito dopo 'price'
# Trova la posizione della colonna 'price' in df2
price_index = df2.columns.get_loc('price') + 1

# Inserisci 'model_name' nella posizione desiderata
df2.insert(price_index, 'model_name', model_col)

# Ora df2 ha 'model_name' subito dopo 'price'
print(df2.head())


#Salva il file modificato
df2.to_csv('./dataset/dataset_job_2/dataset_job2_100k.csv', index=False)
