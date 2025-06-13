import re
# script_convert_to_csv.py
 
input_file = 'C:/Users/hp/Desktop/prova_job1.txt'    # metti qui il nome del tuo file di testo originale
output_file = '/Users/hp/csv_converter/output.csv'  # nome del file csv che verrà creato
 
def normalizza_anni(anni_raw):
    # Elimina spazi e unisce con trattino
    anni = anni_raw.replace(' ', '').split(',')
    return '-'.join(anni)
 
with open(input_file, 'r', encoding='utf-8') as f_in, open(output_file, 'w', encoding='utf-8') as f_out:
    f_out.write('Marca,Modello,Numero_Auto,Prezzo_Basso,Prezzo_Medio,Prezzo_Alto,Anni\n')
 
    for line in f_in:
        line = line.strip()
 
        # Cerca gli ultimi 5 campi (numero, prezzi, anni)
        match = re.search(r'(\d+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d,\s]+)$', line)
        if not match:
            print(f"⚠️ Riga non valida: {line}")
            continue
 
        num_auto, prezzo_basso, prezzo_medio, prezzo_alto, anni_raw = match.groups()
        anni = normalizza_anni(anni_raw)
 
        # Marca = prima parola, Modello = tutto il resto prima dei campi numerici
        model_info = line[:match.start()].strip()
        model_parts = model_info.split()
        marca = model_parts[0]
        modello = ' '.join(model_parts[1:])
 
        # Scrive su CSV
        f_out.write(f'{marca},{modello},{num_auto},{prezzo_basso},{prezzo_medio},{prezzo_alto},{anni}\n')
 
print(f'✅ File \"{output_file}\" creato con successo con anni unificati!')