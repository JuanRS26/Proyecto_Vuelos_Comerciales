import ETL_Functions as ef
from tqdm import tqdm   # Funciona para actualizar la carga en pantalla y saber que porcentaje lleva la ejecucion del script en tiempo real

# Se crea un bucle para saber si el usuario quiere cargar una base de datos
while True:
    Database_Info = input('\nDesea realizar la conexion a una base de datos (Y/N)?: ')

    # Se realiza la conexion a la base de datos
    if Database_Info in ('Y','y'):
        import insert_database as id

        # Se crea una lista con las funciones de la carga de los datos para hacer una carga masiva
        Data_Load  = [id.airline, id.airplane, id.airport, id.description, id.y_1987, id.y_1988, id.y_1989, id.y_1990, id.y_1991, 
                      id.y_1992, id.y_1993, id.y_1994, id.y_1995, id.y_1996, id.y_1997, id.y_1998, id.y_1999, id.y_2000, id.y_2001, 
                      id.y_2002, id.y_2003, id.y_2004, id.y_2005, id.y_2006, id.y_2007, id.y_2008, id.y_2015]

    # Si lo digitado por el usuario no esta en las opciones, se vuelve a pedir que digite una opcion valida
    elif Database_Info not in ('N', 'n') :
        print(f'\nLo que digitaste "{Database_Info}", no es valido\n')
        continue

    break


# Se crea una lista con todos los nombres de los Data sets sin proceso ETL
Data_Sets_ETL = ['airports_1', 'airports_2', '1987', '1988', '1989', '1990', '1991', '1992', '1993', '1994', '1995', '1996', '1997', '1998', '1999', '2000', '2001',
                 '2002', '2003', '2004', '2005', '2006', '2007', '2008',  'flights', 'description', 
                 'carriers', 'plane-data']

# Se crea un bucle while para mostrar al usuario las opciones de ETL que hay 
while True:

    # se crea una variable 'flag' que funciona para identificar cuando el usuario quiere volver al menu principal
    flag = ''

    # Se muetra las opciones que existen en el menu principal y luego se solicita al usuario que digite la opcion que desea 
    print('\n\t\tOpciones\n \
          \n1) Aplicar las tranformaciones a todos los data sets \
          \n2) Aplicar las Transformaciones a un set de datos en especifico \
          \n3) Realizar la carga de todos los datasets a la base de datos \
          \n4) Realizar la carga de datos de un set de datos en especifico \
          \n5) Salir')
    
    opcion = input('\nSelecciona una de las opciones mencionadas: ')

    # En la primera opcion, se hace la tranformacion de los sets de dato de forma masiva
    if opcion == '1':
        print(f'\nla opcion que seleccionaste es {opcion}')
        ef.ETLProcedure(Data_Sets_ETL)
        print('\nSe realizo correctamente las tranformaciones a todos los sets de datos')

    # En la segunda opcion, se realiza las tranformaciones individuales
    elif opcion == '2':

        # Se crea un diccionario con las opciones
        opciones = {'1': '1987',
                    '2': '1988',
                    '3': '1989',
                    '4': '1990',
                    '5': '1991',
                    '6': '1992',
                    '7': '1993',
                    '8': '1994',
                    '9': '1995',
                    '10': '1996',
                    '11': '1997',
                    '12': '1998',
                    '13': '1999',
                    '14': '2000',
                    '15': '2001',
                    '16': '2002',
                    '17': '2003',
                    '18': '2004',
                    '19': '2005',
                    '20': '2006',
                    '21': '2007',
                    '22': '2008',
                    '23': 'flights',
                    '24': ['airports_1', 'airports_2'],
                    '25': 'description',
                    '26': 'carriers',
                    '27': 'plane-data'}

        # Se crea un while con un menu secundario para que el usuario seleccione un data set en especifico
        while True:
            print('\nLa opcion que seleccionaste es 2\n')
            print('\n\t\tSETS DE DATOS \
            \n1) 1987.csv \n2) 1988.csv \n3) 1989.csv \n4) 1990.csv \n5) 1991.csv \n6) 1992.csv \n7) 1993.csv \n8) 1994.csv \
            \n9) 1995.csv \n10) 1996.csv \n11) 1997.csv \n12) 1998.csv \n13) 1999.csv \n14) 2000.csv \n15) 2001.csv \n16) 2002.csv \
            \n17) 2003.csv \n18) 2004.csv \n19) 2005.csv \n20) 2006.csv \n21) 2007.csv \n22) 2008.csv \n23) flights.csv \
            \n24) airports_1.csv y airports_2.csv \n25) description.csv \n26) carriers.csv \n27) plane-data.csv \n28) Volver \n29) Salir')
            
            opcion = input('\nSeleccione el set de datos que quiere aplicar las transformaciones: ')

            # Se verifica que la opcion que el usuario digito exista
            if opcion in opciones:
                name_set = opciones[opcion]

                # Se hace una excepcion con la opcion '24' ya que es necesario usar 2 datasets
                if opcion == '24':
                    print(f'\nLa opcion que seleccionaste fue "{opcion}"\n')
                    ef.ETLProcedure(name_set)
                    print(f'\nSe Realizaron correctamente las transformaciones para el set de datos "airports_1.csv y airports_2.csv"!!')
                else:
                    print(f'\nLa opcion que seleccionaste fue "{opcion}"\n')
                    ef.ETLProcedure([name_set]) # Se procede a enviar al archivo 'ETL_Functions.py' el set de datos que se quiere transformar
                    print(f'\nSe Realizaron correctamente las transformaciones para el set de datos "{name_set}.csv"!!')

            # Si el usuario selecciono la opcion de 'volver' o 'exit' ingresara en esta condicional
            elif opcion in {'28', '29'}:
                print(f'\nLa opcion que seleccionaste fue "{opcion}"\n')
                flag = 'return' if opcion == '28' else 'exit'   # Se reasigna el valor de la variable anteriormente creada para volver al menu principal

            # Si el usuario digita otra cosa que no este en el menu, se vuelve a solicitar que digite una opcion
            else:
                print(f'\nLa opcion "{opcion}" no existe, por favor mira las opciones')
                continue

            break

        # Si el usuario selecciono la opcion se 'volver' la condicional hara que se reinicie el bucle principal 
        if flag == 'return':
            continue

    # En la tercera opcion se hace la carga masiva de todos los data sets luego de tener las transformaciones
    elif opcion == '3':

        # Si el usuario no quizo realizar la conexion a una base de datos, los de vuelve al menu principal
        if Database_Info in ('N', 'n'):
            print('\nNo hay informacion de la base de datos para realizar la carga...\n \
                  \nPor favor seleccione otra opcion...\n')
            continue

        print('\nLa opcion que seleccionaste fue "3"\n')

        #-------------------------------------------------------------------------------------------

        # Número total de iteraciones (simulando un proceso)
        total_iteraciones = 27

        # Inicializar la barra de progreso
        barra_progreso = tqdm(total=total_iteraciones, desc="Procesando")

        #-------------------------------------------------------------------------------------------

        # Se crea un bucle for para ir ejecutando cada una de las funciones que se encuentran en la lista 'Data_Load'
        for func in Data_Load:
            func()
            barra_progreso.update(1)  # Actualizar la barra de progreso

        #-------------------------------------------------------------------------------------------

        # Cerrar la barra de progreso al finalizar
        barra_progreso.close()

        print("Proceso completado.")

        #-------------------------------------------------------------------------------------------


    # En la cuarta opcion se realiza una carga de datos individual
    elif opcion == '4':

        # Si el usuario no quizo realizar la conexion a una base de datos, los de vuelve al menu principal
        if Database_Info in ('N', 'n'):
            print('\nNo hay informacion de la base de datos para realizar la carga...\n \
                  \nPor favor seleccione otra opcion...\n')
            continue

        # Se crea un while con un menu secundario para que el usuario seleccione un data set en especifico
        while True:
            print('\nLa opcion que seleccionaste fue "4"\n')
            print('\n\t\tSETS DE DATOS \
                   \n1) 1987 \n2) 1988 \n3) 1989 \n4) 1990 \n5) 1991 \n6) 1992 \n7) 1993 \n8) 1994 \n9) 1995 \n10) 1996 \n11) 1997 \
                   \n12) 1998 \n13) 1999 \n14) 2000 \n15) 2001 \n16) 2002 \n17) 2003 \n18) 2004 \n19) 2005 \n20) 2006 \n21) 2007 \
                   \n22) 2008 \n23) 2015 \n24) Airports \n25) Description \n26) Airlines \n27) Aiplanes \n28) Volver \n29) Salir')
            
            opcion = input('\nSeleccione el set de datos que quiere aplicar las transformaciones: ')

            # Se crea un diccionario con la funcion correspondiente a cada dataset y su respectivo nombre
            options_mapping = {'1': (id.y_1987, '1987.paquet'),
                               '2': (id.y_1988, '1988.paquet'),
                               '3': (id.y_1989, '1989.paquet'),
                               '4': (id.y_1990, '1990.paquet'),
                               '5': (id.y_1991, '1991.paquet'),
                               '6': (id.y_1992, '1992.paquet'),
                               '7': (id.y_1993, '1993.paquet'),
                               '8': (id.y_1994, '1994.paquet'),
                               '9': (id.y_1995, '1995.paquet'),
                               '10': (id.y_1996, '1996.paquet'),
                               '11': (id.y_1997, '1997.paquet'),
                               '12': (id.y_1998, '1998.paquet'),
                               '13': (id.y_1999, '1999.paquet'),
                               '14': (id.y_2000, '2000.paquet'),
                               '15': (id.y_2001, '2001.paquet'),
                               '16': (id.y_2002, '2002.paquet'),
                               '17': (id.y_2003, '2003.paquet'),
                               '18': (id.y_2004, '2004.paquet'),
                               '19': (id.y_2005, '2005.paquet'),
                               '20': (id.y_2006, '2006.paquet'),
                               '21': (id.y_2007, '2007.paquet'),
                               '22': (id.y_2008, '2008.paquet'),
                               '23': (id.y_2015, '2015.csv'),
                               '24': (id.airport, 'airports.csv'),
                               '25': (id.description, 'description.csv'),
                               '26': (id.airline, 'airlines.csv'),
                               '27': (id.airplane, 'airplanes.csv'),
                               '28': (None, None),  # Volver
                               '29': (None, None)   # Salir
                               }
            
            # Se verifica que la opcion que el usuario digito, exista
            if opcion in options_mapping:
                selected_function, dataset_name = options_mapping[opcion]   # Se asigna la funcion y el nombre en dos variables
                if selected_function is not None:   # Se verifica que el valor de la funcion no sea nulo
                    print(f'\nLa opcion que seleccionaste fue "{opcion}"\n')
                    selected_function() # Se ejecuta la funcion para realizar la carga del data set
                    print(f'\nSe Realizo correctamente la carga de datos del set "{dataset_name}"!!')
                    continue
                else:
                    flag = 'return' if opcion == '28' else 'exit'   # Si el valor de la variable es nulo, se reasiga el valor de 'flag'


            # Si el usuario digita otra cosa que no este en el menu, se vuelve a solicitar que digite una opcion
            else:
                print(f'\nLa opcion "{opcion}" no existe, por favor mira las opciones')
                continue

            break

        # Si el usuario selecciono la opcion se 'volver' la condicional hara que se reinicie el bucle principal 
        if flag == 'return':
            continue

        # continue

    # En la quinta opcion solo se ejecuta un mensaje y termina el ciclo while principal
    elif opcion == '5':
        print(f'\nLa opcion que seleccionaste fue "{opcion}"\n')

    # Si el usuario digita otra cosa que no este en el menu, se vuelve a solicitar que digite una opcion
    else:
        print(f'\nLa opcion "{opcion}" no existe, por mira las opciones')
        continue

    break