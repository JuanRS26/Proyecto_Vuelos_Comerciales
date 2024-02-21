import pandas as pd
import warnings
from tqdm import tqdm   # Funciona para actualizar la carga en pantalla y saber que porcentaje lleva la ejecucion del script en tiempo real

# Desactiva las advertencias de tipo DtypeWarning
warnings.filterwarnings("ignore", category=pd.errors.DtypeWarning)


#----------------------------------- Funcion para cargar los datos sin procesar -----------------------------------

def dataFrames(NameSet):

    # Se comprueba si el dataframe tiene caracteres distintos al protocolo 'utf-8'
    if NameSet in ['airports_1', 'airports_2', 'flights', 'carriers', 'plane-data']:
        try:
            df = pd.read_csv(f'datasets/{NameSet}.csv', encoding = 'utf-8')
        except UnicodeDecodeError:
            df = pd.read_csv(f'datasets/{NameSet}.csv', encoding = 'latin1')    # Se cambia el protocolo de decodificacion

    else:
        df = pd.read_parquet(f'datasets/{NameSet}.parquet')

    return df


# ----------------------------------- Funcion para realizar la tranformacion de cada Set de datos -----------------------------------

def transforms(df, special = False, ColumnTail = 0, CancelReason = False):

    # Se requiere del DataFrame de los aeropuertos para hacer las transformaciones para los registros del 2015
    airports = pd.read_csv('datasets/Transforms/airports.csv')

    # Se crean dos listas, la cual la primera son las columnas que se remplazaran los valores nulos y la segunda para hacer otras transformaciones
    ColumList = ['Sched_Dep_Time', 'Dep_Time','Arr_Time', 'Cancellation_Reason', 'Dep_Delay','Arr_Delay', 'Taxi_Out','Taxi_In', 
                 'Sched_Elapsed_Time', 'Elapsed_Time', 'Air_Time', 'Weather_Delay', 'Nas_Delay', 'Security_Delay', 'Airline_Delay', 
                 'Late_Aircraft_Delay', 'Distance', 'Tail_Num']
    
    DropList = ['Sched_Dep_Time', 'Dep_Time', 'Arr_Time', 'Sched_Arr_Time', 'Cancellation_Reason']

    if special == False:
        # Se crea una lista con los nombres de las columnas con un orden distinto para una mejor visualización para los años entre 1987 a 2008
        new_order_8708 = ['Year', 'Month', 'DayofMonth', 'DayOfWeek', 'UniqueCarrier', 'FlightNum', 'TailNum', 'Origin', 'Dest', 'CRSDepTime', 
                    'DepTime', 'DepDelay', 'CRSArrTime', 'ArrTime', 'ArrDelay', 'TaxiOut', 'TaxiIn', 'CRSElapsedTime', 'ActualElapsedTime', 
                    'AirTime', 'Distance', 'Diverted', 'Cancelled', 'CancellationCode', 'NASDelay', 'SecurityDelay', 'CarrierDelay', 
                    'LateAircraftDelay', 'WeatherDelay']


        # Se crea un diccionario para cambiar el nombre de las columnas para una mejor interprestación para los años entre 1987 a 2008
        rename_columns_8708 = {'Year': 'Year', 'Month': 'Month', 'DayofMonth': 'Day_Of_Month', 'DayOfWeek': 'Day_Of_Week', 
                        'UniqueCarrier': 'Airline_Code', 'FlightNum': 'Flight_Num', 'TailNum': 'Tail_Num', 'Origin': 'Origin_Airport', 
                        'Dest': 'Dest_Airport', 'CRSDepTime': 'Sched_Dep_Time', 'DepTime': 'Dep_Time', 'DepDelay': 'Dep_Delay', 
                        'CRSArrTime': 'Sched_Arr_Time', 'ArrTime': 'Arr_Time', 'ArrDelay': 'Arr_Delay', 'TaxiOut': 'Taxi_Out', 
                        'TaxiIn': 'Taxi_In', 'CRSElapsedTime': 'Sched_Elapsed_Time', 'ActualElapsedTime': 'Elapsed_Time', 
                        'AirTime': 'Air_Time', 'Distance': 'Distance', 'Diverted': 'Diverted', 'Cancelled': 'Cancelled', 
                        'CancellationCode': 'Cancellation_Reason', 'NASDelay': 'Nas_Delay', 'SecurityDelay': 'Security_Delay', 
                        'CarrierDelay': 'Airline_Delay', 'LateAircraftDelay': 'Late_Aircraft_Delay', 'WeatherDelay': 'Weather_Delay'} 
        
        # Se realiza el cambio de orden de las columnas con la lista anteriormente creada
        df = df[new_order_8708]

        # Se realiza el cambio de los nombres de las columnas con el diccionario anteriormente creado
        df.rename(columns = rename_columns_8708, inplace = True)
        
    
    elif special == True:
        # Se crea una lista con los nombres de las columnas del DataFrame con un orden distinto para una mejor visualización
        new_order = ['YEAR','MONTH','DAY','DAY_OF_WEEK','AIRLINE','FLIGHT_NUMBER','TAIL_NUMBER','ORIGIN_AIRPORT','DESTINATION_AIRPORT',
                    'SCHEDULED_DEPARTURE','DEPARTURE_TIME','DEPARTURE_DELAY','SCHEDULED_ARRIVAL','ARRIVAL_TIME', 'ARRIVAL_DELAY',
                    'TAXI_OUT','WHEELS_OFF','WHEELS_ON','TAXI_IN','SCHEDULED_TIME','ELAPSED_TIME', 'AIR_TIME','DISTANCE','DIVERTED',
                    'CANCELLED','CANCELLATION_REASON','AIR_SYSTEM_DELAY','SECURITY_DELAY','AIRLINE_DELAY','LATE_AIRCRAFT_DELAY',
                    'WEATHER_DELAY']
        
        # Se crea un diccionario para posteriormente cambiar el nombre de las columnas para una mejor interprestación
        renames_columns = {'YEAR': 'Year', 'MONTH': 'Month', 'DAY': 'Day_Of_Month', 'DAY_OF_WEEK': 'Day_Of_Week', 'AIRLINE': 'Airline_Code', 
                        'FLIGHT_NUMBER': 'Flight_Num', 'TAIL_NUMBER': 'Tail_Num', 'ORIGIN_AIRPORT': 'Origin_Airport', 
                        'DESTINATION_AIRPORT': 'Dest_Airport', 'SCHEDULED_DEPARTURE': 'Sched_Dep_Time', 'DEPARTURE_TIME': 'Dep_Time', 
                        'DEPARTURE_DELAY': 'Dep_Delay', 'SCHEDULED_ARRIVAL': 'Sched_Arr_Time', 'ARRIVAL_TIME': 'Arr_Time', 
                        'ARRIVAL_DELAY': 'Arr_Delay', 'TAXI_OUT': 'Taxi_Out', 'WHEELS_OFF': 'Wheels_Off', 'WHEELS_ON': 'Wheels_On', 
                        'TAXI_IN': 'Taxi_In', 'SCHEDULED_TIME': 'Sched_Elapsed_Time', 'ELAPSED_TIME': 'Elapsed_Time', 
                        'AIR_TIME': 'Air_Time', 'DISTANCE': 'Distance', 'DIVERTED': 'Diverted', 'CANCELLED': 'Cancelled', 
                        'CANCELLATION_REASON': 'Cancellation_Reason', 'AIR_SYSTEM_DELAY': 'Nas_Delay', 'SECURITY_DELAY': 'Security_Delay', 
                        'AIRLINE_DELAY': 'Airline_Delay', 'LATE_AIRCRAFT_DELAY': 'Late_Aircraft_Delay', 'WEATHER_DELAY': 'Weather_Delay'}

        # Se realiza el cambio de orden de las columnas con la lista anteriormente creada
        df = df[new_order]

        # Se realiza el cambio de los nombres de las columnas con el diccionario anteriormente creado
        df.rename(columns = renames_columns, inplace = True)

        # Se Remplaza los valores nulos con 0
        df['Wheels_Off'].fillna(0, inplace = True)
        df['Wheels_On'].fillna(0, inplace = True)

        # Transformamos los datos para que sea mas legible y se guarda como cadena de texto con el formato correcto de hora
        df['Wheels_Off'] = pd.to_datetime(df['Wheels_Off'].astype(int).astype(str).str.zfill(4), format='%H%M', errors =  'coerce').dt.strftime('%H:%M')
        df['Wheels_On'] = pd.to_datetime(df['Wheels_On'].astype(int).astype(str).str.zfill(4), format='%H%M', errors =  'coerce').dt.strftime('%H:%M')

        df['Wheels_Off'].fillna('00:00', inplace = True)
        df['Wheels_On'].fillna('00:00', inplace = True)


        # Identificar códigos IATA que no están en 'airports' y reemplazarlos con '0'
        df.loc[~df['Origin_Airport'].isin(airports['Code_IATA']), 'Origin_Airport'] = None

        # Identificar códigos IATA que no están en 'airports' y reemplazarlos con '0'
        df.loc[~df['Dest_Airport'].isin(airports['Code_IATA']), 'Dest_Airport'] = None

        # Se elimina los registros sin informacion en la columna 'Origin_Airport' ya que no proporcionan informacion veridica
        df.dropna(subset = ['Origin_Airport'], inplace = True)

        # Se elimina los registros sin informacion en la columna 'Dest_Airport' ya que no proporcionan informacion veridica
        df.dropna(subset = ['Dest_Airport'], inplace = True)


    if ColumnTail == 1:
            # Se elimina la columna 'Tail_Num' de la lista anterior para hacer las transformaciones necesarias
            ColumList.remove('Tail_Num')
            df['Tail_Num'].fillna('0', inplace = True)  # Se remplaza los valores nulos
        
    elif ColumnTail == 2:
        # Utilizar expresiones regulares para mantener solo letras y digitos para la columna 'Tail_Num'
        df['Tail_Num'] = df['Tail_Num'].str.replace(r'[^a-zA-Z0-9]', '', regex=True)
        ColumList.remove('Tail_Num')    # Se elimina la columna 'Tail_Num' de la lista
        df['Tail_Num'].fillna('0', inplace = True)  # Se remplaza los valores nulos

    # Se crea un ciclo que remplaza los valores nulos de las columnas definidas en la lista
    for i in ColumList:
        # print(i)
        df[i].fillna(0, inplace = True)

    # Se crea una nueva lista excluyendo las columnas que tendran una transformacion distinta
    ColumList = [i for i in ColumList if i not in DropList]

    # Se cambia el tipo de dato de la columna 'Cancellation_Reason'
    if CancelReason == False:
        df['Cancellation_Reason'] = df['Cancellation_Reason'].astype(int).astype(str)
    else:
        df['Cancellation_Reason'] = df['Cancellation_Reason'].astype(str)

    # Se realiza un for para cambiar el tipo de dato de las columnas necesarias dentro de la lista 'ColumList'
    for i in ColumList:
        df[i] = df[i].astype(int)

    # Transformamos los datos para que sea mas legible y se guarda como cadena de texto con el formato correcto de hora
    df['Sched_Dep_Time'] = pd.to_datetime(df['Sched_Dep_Time'].astype(str).str.zfill(4), format='%H%M', errors = 'coerce').dt.strftime('%H:%M')
    df['Dep_Time'] = pd.to_datetime(df['Dep_Time'].astype(int).astype(str).str.zfill(4), format='%H%M', errors = 'coerce').dt.strftime('%H:%M')
    df['Arr_Time'] = pd.to_datetime(df['Arr_Time'].astype(int).astype(str).str.zfill(4), format='%H%M', errors = 'coerce').dt.strftime('%H:%M')
    df['Sched_Arr_Time'] = pd.to_datetime(df['Sched_Arr_Time'].astype(str).str.zfill(4), format='%H%M', errors = 'coerce').dt.strftime('%H:%M')

    df['Sched_Dep_Time'].fillna('00:00', inplace = True)
    df['Dep_Time'].fillna('00:00', inplace = True)
    df['Arr_Time'].fillna('00:00', inplace = True)
    df['Sched_Arr_Time'].fillna('00:00', inplace = True)

    return df


#----------------------------------- Funcion para realizar un ETL segun el set de datos ----------------------------------

def ETLProcedure(data):


    #------------------------------- Se crea una barra de prograso -------------------------------

    # Número total de iteraciones (simulando un proceso)
    total_iteraciones = len(data)

    # Inicializar la barra de progreso
    barra_progreso = tqdm(total=total_iteraciones, desc="Procesando")


    #------------------------------- Se inicia con el proceso de ETL para los sets de daos -------------------------------

    grupo1 = ['1987', '1988', '1989', '1990', '1991', '1992', '1993', '1994']
    grupo2 = ['1995', '1996', '1997', '1998', '1999', '2000', '2001']
    grupo3 = ['2003', '2004', '2005', '2006', '2007', '2008']

    bandera = False

    # Se crea un bucle para transformar cada uno de los datasets que se proporcionaron
    for i in range(0, len(data)):

        if data[i] == 'airports_2' or data[i] == 'airports_1':  # Se comprueba que el Dataframe sea el correcto

            if bandera == False:
                if 'airports_2' in data and 'airports_1' in data:

                    # Se cargan dos archivos que contienen datos de los aeropuertos
                    dfAirport1 = pd.read_csv('datasets/airports_1.csv')
                    dfAirport2 = pd.read_csv('datasets/airports_2.csv')

                    # Se renombran las columnas para que los DataFrames sean mas estandar y mas intuitivos
                    dfAirport1.rename(columns = {'IATA_CODE': 'Code_IATA', 'AIRPORT': 'Airport', 'CITY': 'City', 'STATE': 'State', 'COUNTRY': 'Country', 
                                                'LATITUDE': 'LATITUDE', 'LONGITUDE': 'LONGITUDE'}, inplace = True)
                    dfAirport2.rename(columns = {'iata': 'Code_IATA', 'airport': 'Airport', 'city': 'City', 'state': 'State', 'country': 'Country', 
                                                'lat': 'LATITUDE', 'long': 'LONGITUDE'}, inplace = True)

                    # Se unifican ambos DataFrames para tener toda la información en una sola tabla/DataFrame y reiniciamos los index
                    airports = pd.concat([dfAirport1, dfAirport2], ignore_index = True)

                    # Se elimina los datos duplicados tomando como referencia la columna unica 'Code_IATA' y reiniciando los index
                    airports.drop_duplicates(subset = ['Code_IATA'], inplace = True, ignore_index = True)

                    # Se agrega una nueva fila para registros sin codigo
                    New_Row = pd.DataFrame({'Code_IATA': ['0'], 'Airport': ['0'], 'City': ['0'], 'State': ['0'], 'Country': ['0'], 'LATITUDE': [None], 'LONGITUDE': [None]})
                    airports = pd.concat([airports, New_Row], ignore_index = True)

                    # Se remplaza los demas valores nulos o vacion por el numero 0
                    airports.fillna(0, inplace = True)

                    airports.to_csv('datasets/Transforms/airports.csv', index = False)
                    
                bandera = True

            continue


        if data[i] in grupo1:   # Se comprueba que el Dataframe se encuentre en el grupo 1

            df = dataFrames(data[i])
            df = transforms(df)
        
        elif data[i] in grupo2: # Se comprueba que el Dataframe se encuentre en el grupo 2

            df = dataFrames(data[i])
            df = transforms(df, ColumnTail = 1)

        elif data[i] == '2002': # Se comprueba que el Dataframe sea '2002'

            df = dataFrames(data[i])
            df = transforms(df, ColumnTail = 2)
        
        elif data[i] in grupo3: # Se comprueba que el Dataframe se encuentra en el grupo 3

            df = dataFrames(data[i])
            df = transforms(df, ColumnTail = 1, CancelReason = True)
        
        elif data[i] == 'flights':

            df = dataFrames(data[i])
            df = transforms(df, special = True, ColumnTail = 1, CancelReason = True)
            df.to_parquet(f'datasets/Transforms/2015.parquet', index = False)
            continue

        elif data[i] == 'description':

            # Se crea un diccionario con las columnas y su descripcion para reasignarlo al DataFrame 'description'
            new_data = {'Variable': ['Year', 'Month', 'Day_Of_Month', 'Day_Of_Week', 'Airline_Code', 'Flight_Num', 'Tail_Num', 'Origin_Airport', 
                                    'Dest_Airport', 'Sched_Dep_Time', 'Dep_Time', 'Dep_Delay', 'Sched_Arr_Time', 'Arr_Time', 'Arr_Delay', 
                                    'Taxi_Out', 'Wheels_Off', 'Wheels_On', 'Taxi_In', 'Sched_Elapsed_Time', 'Elapsed_Time', 'Air_Time', 'Distance',
                                    'Diverted', 'Cancelled', 'Cancellation_Reason', 'Nas_Delay', 'Security_Delay', 'Airline_Delay', 
                                    'Late_Aircraft_Delay', 'Weather_Delay'],
                        'Description': ['Anio del vuelo', 'Mes del vuelo (1 para enero, 2 para febrero, etc...)', 'Describe el dia del vuelo entre 1 y 31', 
                                        'Dia de la semana del vuelo (1 para lunes, 2 para martes, etc...)', 'Codigo de la aerolinea', 'Numero de vuelo', 
                                        'Numero de cola de la aeronave', 'Codigo del aeropuerto de origen', 'Codigo del aeropuerto de destino', 
                                        'Hora programada de salida', 'Hora real de salida', 'Retraso en la salida (en minutos, positivo si hay retraso)', 
                                        'Hora programada de llegada', 'Hora real de llegada', 'Retraso en la llegada (en minutos, positivo si hay retraso)', 
                                        'Tiempo de rodaje desde la puerta de embarque hasta la pista de despegue',
                                        'Hora en la que las ruedas de la aeronave dejan el suelo',
                                        'Hora en la que las ruedas de la aeronave tocan el suelo', 
                                        'Tiempo de rodaje desde la pista de aterrizaje hasta la puerta de llegada.', 'Tiempo de vuelo programado', 
                                        'Tiempo real transcurrido durante el vuelo', 'Tiempo real en el aire', 'Distancia del vuelo en millas', 
                                        'Desviasion del vuelo, 1 (SI) - 0 (NO)', 'Indicador de si el vuelo fue cancelado. 1 (SI) - 0 (NO)', 
                                        'Motivo de la cancelacion (A = transportista, B = clima C = NAS, D = seguridad)', 
                                        'Tiempo de retraso debido al sistema de control del trafico aereo', 
                                        'Tiempo de retraso debido a problemas de seguridad', 'Tiempo de retraso atribuido a la aerolinea',
                                        'Tiempo de retraso atribuido a la llegada tardia de la aeronave anterior', 
                                        'Tiempo de retraso debido a condiciones climaticas']              
                        }

            # Se reasigna el DataFrame que contiene las nuevas columnas con su respectiva descripción
            df = pd.DataFrame(new_data)

            df.to_csv(f'datasets/Transforms/description.csv', index = False)

            continue
        
        elif data[i] == 'carriers':

            df = dataFrames(data[i])

            # Se renombran las columnas para que sean mas intuitivas
            df.rename(columns = {'Code': 'Code_IATA', 'Description': 'Airline'}, inplace = True)

            # Se remplaza los demas valores nulos o vacion por el numero 0
            df.fillna(0, inplace = True)

            df.to_csv(f'datasets/Transforms/airlines.csv', index = False)

            continue

        elif data[i] == 'plane-data':

            df = dataFrames(data[i])

            # Se eliminan las filas que no tengan información en las columnas 'manufacturer' y 'model' 
            df.dropna(subset = ['manufacturer', 'model'], ignore_index = True, inplace = True)

            # Se remplaza el valor Nan por la fecha de '01/01/1900' en la columna 'issue_date'
            df['issue_date'].fillna('01/01/1900', inplace = True)    

            # Se convierte el tipo de dato de la columna 'issue_date' a dato datetime
            df['issue_date'] = pd.to_datetime(df['issue_date'])   

            # Se extrajo el año de la columna 'issue_date' y se remplazo por los NaN
            df['year'].fillna(df['issue_date'].dt.year, inplace = True)

            df['year'] = df['year'].astype(int)

            # Se remplaza los demas valores nulos o vacion por el numero 0
            df.fillna(0, inplace = True)

            df.to_csv(f'datasets/Transforms/airplanes.csv', index = False)

            continue


        df.to_parquet(f'datasets/Transforms/{data[i]}.parquet', index = False)

        barra_progreso.update(1)  # Actualizar la barra de progreso


    #----------------------------- Fin de la barra de progreso -----------------------------

    # Cerrar la barra de progreso al finalizar
    barra_progreso.close()

    print("Proceso completado.")