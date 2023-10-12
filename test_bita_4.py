import pandas as pd
import psycopg2
import time

'''
POR HACER:

-Cambiar nombre de la tabla stock_big por un nombre nuevo para insertar en tabla nueva stock_big_2
-Ajustar codigo usando Pandas
-Viendo video en youtube: https://www.youtube.com/watch?v=pRGes-EKxUs&ab_channel=CampBI
-Luego ver si cambio orden del codigo, en el sentido de leer primero del csv y luego crear la tabla en la bd. Aunque creo que esta bien 

'''

# OPCION 2 - USANDO PANDAS Y CHUNSIZE 

# Inicio del tiempo de ejecución
inicio = time.time()

# Establecer la conexión con la base de datos PostgreSQL
conn = psycopg2.connect(
    host="localhost",
    port=5433,
    database="bita_db",
    user="postgres",
    password=""
)

# Crear un cursor para ejecutar comandos SQL
cursor = conn.cursor()

# Crear una tabla en PostgreSQL
create_table_query = '''
    CREATE TABLE IF NOT EXISTS stock_big_3 (
        PointOfSale character varying(200),
        Product character varying(200),
        Date date,
        Stock numeric
    )
'''

cursor.execute(create_table_query)
conn.commit()

# Configurar el tamaño de lote deseado. Considerando:  4.554.888 de rows

# Pruebas sin low_memory=False:
# batch_size = 1000 # Prueba 1.   Tiempo de ejecución: 2655.54 seg = 44.25 minutos. Numero de batches = 4554888/1000 = 4554.88 batches.
# batch_size = 10000 # Prueba 2.  Tiempo de ejecución: 2882.39 seg = 48.03 minutos. Numero de batches = 4554888/10000 = 455.48 batches.
# batch_size = 100000 # Prueba 3. Tiempo de ejecución: 2291.52 seg = 38.192 minutos. Numero de batches = 4554888/100000 = 45.54 batches. 

# Pruebas con low_memory=False:
# batch_size = 1000000  #Prueba 4. Tiempo de ejecución: 2478.46 seg. = 41.30 minutos. Numero de batches = 4554888/1000000 = 4.55 batches. 
# batch_size = 1000     #Prueba 5. Tiempo de ejecución: 2642.444 seg = 44.04 minutos. Numero de batches = 4554888/1000 = 4554.88 batches.
batch_size = 1000 

# Pruebas corriendo. test_bita_2.py (Tabla: stock_big)

    # Prueba 0. SIN reader.line_num % 10000 == 0: Tiempo de ejecución: 670.4795563220978 seg = 11.17min
    # Prueba 1. if reader.line_num % 10000 == 0:  Tiempo de ejecución: 693.92 seg = 11.5min 
    # Prueba 5. if reader.line_num % 500 == 0:    Tiempo de ejecución: 698.783 seg = 11.64 min 

# Pruebas corriendo. test_bita_3.py (Tabla: stock_big_tb3)

    # Prueba 2. if reader.line_num % 1000 == 0:   Tiempo de ejecución: 666.56 seg = 11.10 min
    # Prueba 3. if reader.line_num % 100000 == 0: Tiempo de ejecución: 870.34 seg = 14.5 min 
    # Prueba 4. if reader.line_num % 1000000 == 0: Tiempo de ejecución: 672.39 seg == 11.2min

# Conclusiones:

    # Prueba 0. SIN reader.line_num % 10000 == 0: Tiempo de ejecución: 670.4795563220978 seg = 11.17min
    # Prueba 5. if reader.line_num % 500 == 0:    Tiempo de ejecución: 698.783 seg = 11.64 min 
    # Prueba 2. if reader.line_num % 1000 == 0:   Tiempo de ejecución: 666.56 seg  = 11.10 min --Mejor opcion cada 1000 registros 
    # Prueba 1. if reader.line_num % 10000 == 0:  Tiempo de ejecución: 693.92 seg  = 11.5min
    # Prueba 3. if reader.line_num % 100000 == 0: Tiempo de ejecución: 870.34 seg  = 14.5 min 
    # Prueba 4. if reader.line_num % 1000000 == 0: Tiempo de ejecución: 672.39 seg = 11.2min 

ACTUAL:

    1. ACTUALIZADO EN test_bita_3.py con reader.line_num % 1000 == 0 # Me parece mejor solucion por los momentos 

    2. CREE test_bita_3_1, donde realizo lo siguiente:
        -En el csv elimino los casos que no tengan "_" y sobreescribo ese csv
        -Ahora el csv a leer para insertar tendra solo los registros que tengan el caracter "_", con eso ya no tengo que aplicar filtro en el codigo 
    IDEA: COMPARAR EL TIEMPO DE EJECUCION DE
        test_bita_3   -> filtro directo en el codigo sin ajustar csv original 
        test_bita_3_1 -> sobcreescribiendo el csv ya con filtro aplicado (En esta prueba se utilizo otro archivo csv -> stock_ajustando.csv)
            test_bita_ 3 . Tiempo de ejecución: 718.74 seg = 11.97 minutos 
            test_bita_3_1. Tiempo de ejecución: 667.08 seg = 11.11 minutos 

            test_bita_ 3 . Tiempo de ejecución: corriendo again 
            test_bita_3_1. Tiempo de ejecución: volver a correrlo, cambiando csv de nuevo y borrando tabla de la bd 

    3. EXPLICAR SOLUCION test_bita_4.py usando libreria pandas y mostrando casos de prueba que realice...

    4. SUBIR LAS 3 OPCIONES QUE TENGO, README CON EXPLICACION Y RESULTADOS Y TIMER PARA PROBAR...

# Luego hacer prueba 4 y hacer la prueba utilizando el COPY DE POSTGRESQL SOLO POR PROBAR 

# Intentar probar por lotes de 500 o 100, capaz el rendimiento mejora. Igualmente no veo mayor mejoria que el codigo original
# Tendre que buscar otras formas de procesar datos o librerias recomendadas
# Si vuelvo a la forma inicial, tratar de optimizarlo aun mas...

'''
    La idea de insertar por lotes, es insertar primero el numero de registros que estan en batch_size por ejemplo de 1000 en 1000
    No se carga toda la tabla en memoria, sino se carga cada 1000 registros

    Ver videos: 

        https://www.youtube.com/watch?v=pRGes-EKxUs&ab_channel=CampBI -> Aplican lo del chunk que estoy haciendo

        -Recordar lo siguiente:
            -Si consulto a la vez sobre la base de datos que se esta insertando, puedo perjudicar rendimiento
            -Si ejecuto las otras pruebas en paralelo, podrian perjudicar rendimiento
            -Hare cada prueba separada y anotare resultados...
            -ANTES DE VOLVER A EJECUTAR SCRIPT, borrar tabla stock_big_3

CONCLUSIONES PARA CUANDO VAYA A EXPLICAR POR QUE TOME UNA DECISION Y NO OTRA, INCLUSO MOSTRAR LOS CODIGOS Y PRUEBAS QUE HICE:


'''
low_memory=False
# Leer el archivo CSV utilizando Pandas en lotes
#for chunk in pd.read_csv('C:/Users/fabia/Desktop/stock_short.csv', delimiter=';', chunksize=batch_size): #Test con el archivo corto
for chunk in pd.read_csv('C:/Users/fabia/Desktop/Stock.csv', delimiter=';', chunksize=batch_size): #Test con el archivo largo

    # Filtrar los datos del chunk donde el campo "PointOfSale" contiene el carácter "_"
    filtered_chunk = chunk[chunk['PointOfSale'].astype(str).str.contains('_')]

    # Insertar los datos filtrados en la tabla en lotes pequeños
    for index, row in filtered_chunk.iterrows():
        insert_query = '''
            INSERT INTO stock_big_3 (PointOfSale, Product, Date, Stock)
            VALUES (%s, %s, %s, %s)
        '''

        values = (row['PointOfSale'], row['Product'], row['Date'], row['Stock'])
        cursor.execute(insert_query, values)
        conn.commit()

# Cerrar el cursor y la conexión
cursor.close()
conn.close()

# Fin del tiempo de ejecución
fin = time.time()

# Tiempo total de ejecución
tiempo_total = fin - inicio

print("Tiempo de ejecución:", tiempo_total, "segundos")