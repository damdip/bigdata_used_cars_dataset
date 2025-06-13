import pandas as pd


# Legge il CSV
df = pd.read_csv('dataset/dataset_job_2/input_mr_job_2_1M.csv')

# Mostra le prime 5 righe
print(df.tail())
