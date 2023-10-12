# CODIGO PARA IMPRIMIR ELEMENTOS DE UN CSV - FORMA 2 

import csv

#with open('C:/Users/fabia/Desktop/Stock.csv', 'r') as file:
with open('C:/Users/fabia/Desktop/stock_ajustando.csv', 'r') as file:
    reader = csv.reader(file)
    next(reader)  # Ignorar la primera fila si contiene encabezados

    for row in reader:
        if "_" in row[0]:
            print(row)

