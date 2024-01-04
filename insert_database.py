import pandas as pd
import mysql.connector


# --------------- Se realiza la carga de la informacion de la base de datos ------------

try:
      df = pd.read_json('database/data_host.json')
except FileNotFoundError:

      while True:
            print('\nNo se encontro informacion de la base de datos\n')
            opcion = input('Quiere ingresar la informacion de una base de datos personal? (y/n): ')

            if opcion in {'y', 'Y'}:
                  data = {
                        'host': [],
                        'user': [],
                        'pass': [],
                        'database': []
                  }

                  data['host'].append(input('Digite el host de la base de datos: '))
                  data['user'].append(input('Digite el nombre del usuario: '))
                  data['pass'].append(input('Digite la contrasenia del usuario: '))
                  data['database'].append(input('Digite el nombre de la base de datos: '))

                  df = pd.DataFrame(data)
                  df.to_json('database/data_host.json') 

                  df = pd.read_json('database/data_host.json')
                  break
            else:
               print('\nPorfavor ingresa informacion necesaria para ingresar a la base de datos\n')


# ----------------------------------------------------------------------------------


mydb = mysql.connector.connect(
  host=df['host'][0],   # Se pone la direccion IP donde se aloja la base de datos 
  user=df['user'][0],        # Se pone el nombre del usurio que hara las consultas a la base de datos 
  password=df['pass'][0],    # Se pone la contraseña del usuario en caso que tenga una 
  database=df['database'][0]      # Se pone el nombre de la base de datos a la que se quiere hacer las consultas
)

mycursor = mydb.cursor()      # Se crea un cursor para poder ejecutar consultas a la base de datos


# Year 1987

# Se crea una funcion para cargar el set de datos a la tabla asiganada
def y_1987():
   
  # Se carga los datos del data set
  df1 = pd.read_parquet('datasets/Transforms/1987.parquet')

  # Se crea u bucle for para hacer la carga de los datos a la base de datos donde 'sql': es el protocolo y 'val': de donde provienen los datos    
  for i in range(0, len(df1)):
      sql = 'INSERT INTO `y_1987` VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
      val = (int(df1.loc[i][0]), int(df1.loc[i][1]), int(df1.loc[i][2]), int(df1.loc[i][3]), df1.loc[i][4], int(df1.loc[i][5]), 
            int(df1.loc[i][6]), df1.loc[i][7], df1.loc[i][8], df1.loc[i][9], df1.loc[i][10], int(df1.loc[i][11]), df1.loc[i][12], 
            df1.loc[i][13], int(df1.loc[i][14]), int(df1.loc[i][15]), int(df1.loc[i][16]), int(df1.loc[i][17]), int(df1.loc[i][18]), 
            int(df1.loc[i][19]), int(df1.loc[i][20]), int(df1.loc[i][21]), int(df1.loc[i][22]), int(df1.loc[i][23]), int(df1.loc[i][24]),
            int(df1.loc[i][25]), int(df1.loc[i][26]), int(df1.loc[i][27]), int(df1.loc[i][28]))

      mycursor.execute(sql, val)    # Se ejecuta la consulta
      mydb.commit()     # Se guardan los cambio


# Year 1988

# Se crea una funcion para cargar el set de datos a la tabla asiganada
def y_1988():
  
  # Se carga los datos del data set
  df1 = pd.read_parquet('datasets/Transforms/1988.parquet')

  # Se crea u bucle for para hacer la carga de los datos a la base de datos donde 'sql': es el protocolo y 'val': de donde provienen los datos
  for i in range(0, len(df1)):
      sql = 'INSERT INTO `y_1988` VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
      val = (int(df1.loc[i][0]), int(df1.loc[i][1]), int(df1.loc[i][2]), int(df1.loc[i][3]), df1.loc[i][4], int(df1.loc[i][5]), 
            int(df1.loc[i][6]), df1.loc[i][7], df1.loc[i][8], df1.loc[i][9], df1.loc[i][10], int(df1.loc[i][11]), df1.loc[i][12], 
            df1.loc[i][13], int(df1.loc[i][14]), int(df1.loc[i][15]), int(df1.loc[i][16]), int(df1.loc[i][17]), int(df1.loc[i][18]), 
            int(df1.loc[i][19]), int(df1.loc[i][20]), int(df1.loc[i][21]), int(df1.loc[i][22]), int(df1.loc[i][23]), int(df1.loc[i][24]),
            int(df1.loc[i][25]), int(df1.loc[i][26]), int(df1.loc[i][27]), int(df1.loc[i][28]))

      mycursor.execute(sql, val)    # Se ejecuta la consulta
      mydb.commit()     # Se guardan los cambio


# Year 1989

# Se crea una funcion para cargar el set de datos a la tabla asiganada
def y_1989():
  
  # Se carga los datos del data set
  df1 = pd.read_parquet('datasets/Transforms/1989.parquet')

  # Se crea u bucle for para hacer la carga de los datos a la base de datos donde 'sql': es el protocolo y 'val': de donde provienen los datos
  for i in range(0, len(df1)):
      sql = 'INSERT INTO `y_1989` VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
      val = (int(df1.loc[i][0]), int(df1.loc[i][1]), int(df1.loc[i][2]), int(df1.loc[i][3]), df1.loc[i][4], int(df1.loc[i][5]), 
            int(df1.loc[i][6]), df1.loc[i][7], df1.loc[i][8], df1.loc[i][9], df1.loc[i][10], int(df1.loc[i][11]), df1.loc[i][12], 
            df1.loc[i][13], int(df1.loc[i][14]), int(df1.loc[i][15]), int(df1.loc[i][16]), int(df1.loc[i][17]), int(df1.loc[i][18]), 
            int(df1.loc[i][19]), int(df1.loc[i][20]), int(df1.loc[i][21]), int(df1.loc[i][22]), int(df1.loc[i][23]), int(df1.loc[i][24]),
            int(df1.loc[i][25]), int(df1.loc[i][26]), int(df1.loc[i][27]), int(df1.loc[i][28]))

      mycursor.execute(sql, val)    # Se ejecuta la consulta
      mydb.commit()     # Se guardan los cambio


# Year 1990

# Se crea una funcion para cargar el set de datos a la tabla asiganada
def y_1990():
  
  # Se carga los datos del data set
  df1 = pd.read_parquet('datasets/Transforms/1990.parquet')

  # Se crea u bucle for para hacer la carga de los datos a la base de datos donde 'sql': es el protocolo y 'val': de donde provienen los datos
  for i in range(0, len(df1)):
      sql = 'INSERT INTO `y_1990` VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
      val = (int(df1.loc[i][0]), int(df1.loc[i][1]), int(df1.loc[i][2]), int(df1.loc[i][3]), df1.loc[i][4], int(df1.loc[i][5]), 
            int(df1.loc[i][6]), df1.loc[i][7], df1.loc[i][8], df1.loc[i][9], df1.loc[i][10], int(df1.loc[i][11]), df1.loc[i][12], 
            df1.loc[i][13], int(df1.loc[i][14]), int(df1.loc[i][15]), int(df1.loc[i][16]), int(df1.loc[i][17]), int(df1.loc[i][18]), 
            int(df1.loc[i][19]), int(df1.loc[i][20]), int(df1.loc[i][21]), int(df1.loc[i][22]), int(df1.loc[i][23]), int(df1.loc[i][24]),
            int(df1.loc[i][25]), int(df1.loc[i][26]), int(df1.loc[i][27]), int(df1.loc[i][28]))

      mycursor.execute(sql, val)    # Se ejecuta la consulta
      mydb.commit()     # Se guardan los cambio


# Year 1991

# Se crea una funcion para cargar el set de datos a la tabla asiganada
def y_1991():
  
  # Se carga los datos del data set
  df1 = pd.read_parquet('datasets/Transforms/1991.parquet')

  # Se crea u bucle for para hacer la carga de los datos a la base de datos donde 'sql': es el protocolo y 'val': de donde provienen los datos
  for i in range(0, len(df1)):
      sql = 'INSERT INTO `y_1991` VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
      val = (int(df1.loc[i][0]), int(df1.loc[i][1]), int(df1.loc[i][2]), int(df1.loc[i][3]), df1.loc[i][4], int(df1.loc[i][5]), 
            int(df1.loc[i][6]), df1.loc[i][7], df1.loc[i][8], df1.loc[i][9], df1.loc[i][10], int(df1.loc[i][11]), df1.loc[i][12], 
            df1.loc[i][13], int(df1.loc[i][14]), int(df1.loc[i][15]), int(df1.loc[i][16]), int(df1.loc[i][17]), int(df1.loc[i][18]), 
            int(df1.loc[i][19]), int(df1.loc[i][20]), int(df1.loc[i][21]), int(df1.loc[i][22]), int(df1.loc[i][23]), int(df1.loc[i][24]),
            int(df1.loc[i][25]), int(df1.loc[i][26]), int(df1.loc[i][27]), int(df1.loc[i][28]))

      mycursor.execute(sql, val)    # Se ejecuta la consulta
      mydb.commit()     # Se guardan los cambio


# Year 1992

# Se crea una funcion para cargar el set de datos a la tabla asiganada
def y_1992():
  
  # Se carga los datos del data set
  df1 = pd.read_parquet('datasets/Transforms/1992.parquet')

  # Se crea u bucle for para hacer la carga de los datos a la base de datos donde 'sql': es el protocolo y 'val': de donde provienen los datos
  for i in range(0, len(df1)):
      sql = 'INSERT INTO `y_1992` VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
      val = (int(df1.loc[i][0]), int(df1.loc[i][1]), int(df1.loc[i][2]), int(df1.loc[i][3]), df1.loc[i][4], int(df1.loc[i][5]), 
            int(df1.loc[i][6]), df1.loc[i][7], df1.loc[i][8], df1.loc[i][9], df1.loc[i][10], int(df1.loc[i][11]), df1.loc[i][12], 
            df1.loc[i][13], int(df1.loc[i][14]), int(df1.loc[i][15]), int(df1.loc[i][16]), int(df1.loc[i][17]), int(df1.loc[i][18]), 
            int(df1.loc[i][19]), int(df1.loc[i][20]), int(df1.loc[i][21]), int(df1.loc[i][22]), int(df1.loc[i][23]), int(df1.loc[i][24]),
            int(df1.loc[i][25]), int(df1.loc[i][26]), int(df1.loc[i][27]), int(df1.loc[i][28]))

      mycursor.execute(sql, val)    # Se ejecuta la consulta
      mydb.commit()     # Se guardan los cambio


# Year 1993

# Se crea una funcion para cargar el set de datos a la tabla asiganada
def y_1993():
  
  # Se carga los datos del data set
  df1 = pd.read_parquet('datasets/Transforms/1993.parquet')

  # Se crea u bucle for para hacer la carga de los datos a la base de datos donde 'sql': es el protocolo y 'val': de donde provienen los datos
  for i in range(0, len(df1)):
      sql = 'INSERT INTO `y_1993` VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
      val = (int(df1.loc[i][0]), int(df1.loc[i][1]), int(df1.loc[i][2]), int(df1.loc[i][3]), df1.loc[i][4], int(df1.loc[i][5]), 
            int(df1.loc[i][6]), df1.loc[i][7], df1.loc[i][8], df1.loc[i][9], df1.loc[i][10], int(df1.loc[i][11]), df1.loc[i][12], 
            df1.loc[i][13], int(df1.loc[i][14]), int(df1.loc[i][15]), int(df1.loc[i][16]), int(df1.loc[i][17]), int(df1.loc[i][18]), 
            int(df1.loc[i][19]), int(df1.loc[i][20]), int(df1.loc[i][21]), int(df1.loc[i][22]), int(df1.loc[i][23]), int(df1.loc[i][24]),
            int(df1.loc[i][25]), int(df1.loc[i][26]), int(df1.loc[i][27]), int(df1.loc[i][28]))

      mycursor.execute(sql, val)    # Se ejecuta la consulta
      mydb.commit()     # Se guardan los cambio


# Year 1994

# Se crea una funcion para cargar el set de datos a la tabla asiganada
def y_1994():
  
  # Se carga los datos del data set
  df1 = pd.read_parquet('datasets/Transforms/1994.parquet')

  # Se crea u bucle for para hacer la carga de los datos a la base de datos donde 'sql': es el protocolo y 'val': de donde provienen los datos
  for i in range(0, len(df1)):
      sql = 'INSERT INTO `y_1994` VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
      val = (int(df1.loc[i][0]), int(df1.loc[i][1]), int(df1.loc[i][2]), int(df1.loc[i][3]), df1.loc[i][4], int(df1.loc[i][5]), 
            int(df1.loc[i][6]), df1.loc[i][7], df1.loc[i][8], df1.loc[i][9], df1.loc[i][10], int(df1.loc[i][11]), df1.loc[i][12], 
            df1.loc[i][13], int(df1.loc[i][14]), int(df1.loc[i][15]), int(df1.loc[i][16]), int(df1.loc[i][17]), int(df1.loc[i][18]), 
            int(df1.loc[i][19]), int(df1.loc[i][20]), int(df1.loc[i][21]), int(df1.loc[i][22]), int(df1.loc[i][23]), int(df1.loc[i][24]),
            int(df1.loc[i][25]), int(df1.loc[i][26]), int(df1.loc[i][27]), int(df1.loc[i][28]))

      mycursor.execute(sql, val)    # Se ejecuta la consulta
      mydb.commit()     # Se guardan los cambio


# Year 1995

# Se crea una funcion para cargar el set de datos a la tabla asiganada
def y_1995():
  
  # Se carga los datos del data set
  df1 = pd.read_parquet('datasets/Transforms/1995.parquet')

  # Se crea u bucle for para hacer la carga de los datos a la base de datos donde 'sql': es el protocolo y 'val': de donde provienen los datos
  for i in range(0, len(df1)):
      sql = 'INSERT INTO `y_1995` VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
      val = (int(df1.loc[i][0]), int(df1.loc[i][1]), int(df1.loc[i][2]), int(df1.loc[i][3]), df1.loc[i][4], int(df1.loc[i][5]), 
            df1.loc[i][6], df1.loc[i][7], df1.loc[i][8], df1.loc[i][9], df1.loc[i][10], int(df1.loc[i][11]), df1.loc[i][12], 
            df1.loc[i][13], int(df1.loc[i][14]), int(df1.loc[i][15]), int(df1.loc[i][16]), int(df1.loc[i][17]), int(df1.loc[i][18]), 
            int(df1.loc[i][19]), int(df1.loc[i][20]), int(df1.loc[i][21]), int(df1.loc[i][22]), int(df1.loc[i][23]), int(df1.loc[i][24]),
            int(df1.loc[i][25]), int(df1.loc[i][26]), int(df1.loc[i][27]), int(df1.loc[i][28]))

      mycursor.execute(sql, val)    # Se ejecuta la consulta
      mydb.commit()     # Se guardan los cambio


# Year 1996

# Se crea una funcion para cargar el set de datos a la tabla asiganada
def y_1996():
  
  # Se carga los datos del data set
  df1 = pd.read_parquet('datasets/Transforms/1996.parquet')

  # Se crea u bucle for para hacer la carga de los datos a la base de datos donde 'sql': es el protocolo y 'val': de donde provienen los datos
  for i in range(0, len(df1)):
      sql = 'INSERT INTO `y_1996` VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
      val = (int(df1.loc[i][0]), int(df1.loc[i][1]), int(df1.loc[i][2]), int(df1.loc[i][3]), df1.loc[i][4], int(df1.loc[i][5]), 
            df1.loc[i][6], df1.loc[i][7], df1.loc[i][8], df1.loc[i][9], df1.loc[i][10], int(df1.loc[i][11]), df1.loc[i][12], 
            df1.loc[i][13], int(df1.loc[i][14]), int(df1.loc[i][15]), int(df1.loc[i][16]), int(df1.loc[i][17]), int(df1.loc[i][18]), 
            int(df1.loc[i][19]), int(df1.loc[i][20]), int(df1.loc[i][21]), int(df1.loc[i][22]), int(df1.loc[i][23]), int(df1.loc[i][24]),
            int(df1.loc[i][25]), int(df1.loc[i][26]), int(df1.loc[i][27]), int(df1.loc[i][28]))

      mycursor.execute(sql, val)    # Se ejecuta la consulta
      mydb.commit()     # Se guardan los cambio


# Year 1997

# Se crea una funcion para cargar el set de datos a la tabla asiganada
def y_1997():
  
  # Se carga los datos del data set
  df1 = pd.read_parquet('datasets/Transforms/1997.parquet')

  # Se crea u bucle for para hacer la carga de los datos a la base de datos donde 'sql': es el protocolo y 'val': de donde provienen los datos
  for i in range(0, len(df1)):
      sql = 'INSERT INTO `y_1997` VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
      val = (int(df1.loc[i][0]), int(df1.loc[i][1]), int(df1.loc[i][2]), int(df1.loc[i][3]), df1.loc[i][4], int(df1.loc[i][5]), 
            df1.loc[i][6], df1.loc[i][7], df1.loc[i][8], df1.loc[i][9], df1.loc[i][10], int(df1.loc[i][11]), df1.loc[i][12], 
            df1.loc[i][13], int(df1.loc[i][14]), int(df1.loc[i][15]), int(df1.loc[i][16]), int(df1.loc[i][17]), int(df1.loc[i][18]), 
            int(df1.loc[i][19]), int(df1.loc[i][20]), int(df1.loc[i][21]), int(df1.loc[i][22]), int(df1.loc[i][23]), int(df1.loc[i][24]),
            int(df1.loc[i][25]), int(df1.loc[i][26]), int(df1.loc[i][27]), int(df1.loc[i][28]))

      mycursor.execute(sql, val)    # Se ejecuta la consulta
      mydb.commit()     # Se guardan los cambio


# Year 1998

# Se crea una funcion para cargar el set de datos a la tabla asiganada
def y_1998():
  
  # Se carga los datos del data set
  df1 = pd.read_parquet('datasets/Transforms/1998.parquet')

  # Se crea u bucle for para hacer la carga de los datos a la base de datos donde 'sql': es el protocolo y 'val': de donde provienen los datos
  for i in range(0, len(df1)):
      sql = 'INSERT INTO `y_1998` VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
      val = (int(df1.loc[i][0]), int(df1.loc[i][1]), int(df1.loc[i][2]), int(df1.loc[i][3]), df1.loc[i][4], int(df1.loc[i][5]), 
            df1.loc[i][6], df1.loc[i][7], df1.loc[i][8], df1.loc[i][9], df1.loc[i][10], int(df1.loc[i][11]), df1.loc[i][12], 
            df1.loc[i][13], int(df1.loc[i][14]), int(df1.loc[i][15]), int(df1.loc[i][16]), int(df1.loc[i][17]), int(df1.loc[i][18]), 
            int(df1.loc[i][19]), int(df1.loc[i][20]), int(df1.loc[i][21]), int(df1.loc[i][22]), int(df1.loc[i][23]), int(df1.loc[i][24]),
            int(df1.loc[i][25]), int(df1.loc[i][26]), int(df1.loc[i][27]), int(df1.loc[i][28]))

      mycursor.execute(sql, val)    # Se ejecuta la consulta
      mydb.commit()     # Se guardan los cambio


# Year 1999

# Se crea una funcion para cargar el set de datos a la tabla asiganada
def y_1999():
  
  # Se carga los datos del data set
  df1 = pd.read_parquet('datasets/Transforms/1999.parquet')

  # Se crea u bucle for para hacer la carga de los datos a la base de datos donde 'sql': es el protocolo y 'val': de donde provienen los datos
  for i in range(0, len(df1)):
      sql = 'INSERT INTO `y_1999` VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
      val = (int(df1.loc[i][0]), int(df1.loc[i][1]), int(df1.loc[i][2]), int(df1.loc[i][3]), df1.loc[i][4], int(df1.loc[i][5]), 
            df1.loc[i][6], df1.loc[i][7], df1.loc[i][8], df1.loc[i][9], df1.loc[i][10], int(df1.loc[i][11]), df1.loc[i][12], 
            df1.loc[i][13], int(df1.loc[i][14]), int(df1.loc[i][15]), int(df1.loc[i][16]), int(df1.loc[i][17]), int(df1.loc[i][18]), 
            int(df1.loc[i][19]), int(df1.loc[i][20]), int(df1.loc[i][21]), int(df1.loc[i][22]), int(df1.loc[i][23]), int(df1.loc[i][24]),
            int(df1.loc[i][25]), int(df1.loc[i][26]), int(df1.loc[i][27]), int(df1.loc[i][28]))

      mycursor.execute(sql, val)    # Se ejecuta la consulta
      mydb.commit()     # Se guardan los cambio


# Year 2000

# Se crea una funcion para cargar el set de datos a la tabla asiganada
def y_2000():
  
  # Se carga los datos del data set
  df1 = pd.read_parquet('datasets/Transforms/2000.parquet')

  # Se crea u bucle for para hacer la carga de los datos a la base de datos donde 'sql': es el protocolo y 'val': de donde provienen los datos
  for i in range(0, len(df1)):
      sql = 'INSERT INTO `y_2000` VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
      val = (int(df1.loc[i][0]), int(df1.loc[i][1]), int(df1.loc[i][2]), int(df1.loc[i][3]), df1.loc[i][4], int(df1.loc[i][5]), 
            df1.loc[i][6], df1.loc[i][7], df1.loc[i][8], df1.loc[i][9], df1.loc[i][10], int(df1.loc[i][11]), df1.loc[i][12], 
            df1.loc[i][13], int(df1.loc[i][14]), int(df1.loc[i][15]), int(df1.loc[i][16]), int(df1.loc[i][17]), int(df1.loc[i][18]), 
            int(df1.loc[i][19]), int(df1.loc[i][20]), int(df1.loc[i][21]), int(df1.loc[i][22]), int(df1.loc[i][23]), int(df1.loc[i][24]),
            int(df1.loc[i][25]), int(df1.loc[i][26]), int(df1.loc[i][27]), int(df1.loc[i][28]))

      mycursor.execute(sql, val)    # Se ejecuta la consulta
      mydb.commit()     # Se guardan los cambio


# Year 2001

# Se crea una funcion para cargar el set de datos a la tabla asiganada
def y_2001():
  
  # Se carga los datos del data set
  df1 = pd.read_parquet('datasets/Transforms/2001.parquet')

  # Se crea u bucle for para hacer la carga de los datos a la base de datos donde 'sql': es el protocolo y 'val': de donde provienen los datos
  for i in range(0, len(df1)):
      sql = 'INSERT INTO `y_2001` VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
      val = (int(df1.loc[i][0]), int(df1.loc[i][1]), int(df1.loc[i][2]), int(df1.loc[i][3]), df1.loc[i][4], int(df1.loc[i][5]), 
            df1.loc[i][6], df1.loc[i][7], df1.loc[i][8], df1.loc[i][9], df1.loc[i][10], int(df1.loc[i][11]), df1.loc[i][12], 
            df1.loc[i][13], int(df1.loc[i][14]), int(df1.loc[i][15]), int(df1.loc[i][16]), int(df1.loc[i][17]), int(df1.loc[i][18]), 
            int(df1.loc[i][19]), int(df1.loc[i][20]), int(df1.loc[i][21]), int(df1.loc[i][22]), int(df1.loc[i][23]), int(df1.loc[i][24]),
            int(df1.loc[i][25]), int(df1.loc[i][26]), int(df1.loc[i][27]), int(df1.loc[i][28]))

      mycursor.execute(sql, val)    # Se ejecuta la consulta
      mydb.commit()     # Se guardan los cambio


# Year 2002

# Se crea una funcion para cargar el set de datos a la tabla asiganada
def y_2002():
  
  # Se carga los datos del data set
  df1 = pd.read_parquet('datasets/Transforms/2002.parquet')

  # Se crea u bucle for para hacer la carga de los datos a la base de datos donde 'sql': es el protocolo y 'val': de donde provienen los datos
  for i in range(0, len(df1)):
      sql = 'INSERT INTO `y_2002` VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
      val = (int(df1.loc[i][0]), int(df1.loc[i][1]), int(df1.loc[i][2]), int(df1.loc[i][3]), df1.loc[i][4], int(df1.loc[i][5]), 
            df1.loc[i][6], df1.loc[i][7], df1.loc[i][8], df1.loc[i][9], df1.loc[i][10], int(df1.loc[i][11]), df1.loc[i][12], 
            df1.loc[i][13], int(df1.loc[i][14]), int(df1.loc[i][15]), int(df1.loc[i][16]), int(df1.loc[i][17]), int(df1.loc[i][18]), 
            int(df1.loc[i][19]), int(df1.loc[i][20]), int(df1.loc[i][21]), int(df1.loc[i][22]), int(df1.loc[i][23]), int(df1.loc[i][24]),
            int(df1.loc[i][25]), int(df1.loc[i][26]), int(df1.loc[i][27]), int(df1.loc[i][28]))

      mycursor.execute(sql, val)    # Se ejecuta la consulta
      mydb.commit()     # Se guardan los cambio


# Year 2003

# Se crea una funcion para cargar el set de datos a la tabla asiganada
def y_2003():
  
  # Se carga los datos del data set
  df1 = pd.read_parquet('datasets/Transforms/2003.parquet')

  # Se crea u bucle for para hacer la carga de los datos a la base de datos donde 'sql': es el protocolo y 'val': de donde provienen los datos
  for i in range(0, len(df1)):
      sql = 'INSERT INTO `y_2003` VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
      val = (int(df1.loc[i][0]), int(df1.loc[i][1]), int(df1.loc[i][2]), int(df1.loc[i][3]), df1.loc[i][4], int(df1.loc[i][5]), 
            df1.loc[i][6], df1.loc[i][7], df1.loc[i][8], df1.loc[i][9], df1.loc[i][10], int(df1.loc[i][11]), df1.loc[i][12], 
            df1.loc[i][13], int(df1.loc[i][14]), int(df1.loc[i][15]), int(df1.loc[i][16]), int(df1.loc[i][17]), int(df1.loc[i][18]), 
            int(df1.loc[i][19]), int(df1.loc[i][20]), int(df1.loc[i][21]), int(df1.loc[i][22]), df1.loc[i][23], int(df1.loc[i][24]),
            int(df1.loc[i][25]), int(df1.loc[i][26]), int(df1.loc[i][27]), int(df1.loc[i][28]))

      mycursor.execute(sql, val)    # Se ejecuta la consulta
      mydb.commit()     # Se guardan los cambio


# Year 2004

# Se crea una funcion para cargar el set de datos a la tabla asiganada
def y_2004():
  
  # Se carga los datos del data set
  df1 = pd.read_parquet('datasets/Transforms/2004.parquet')

  # Se crea u bucle for para hacer la carga de los datos a la base de datos donde 'sql': es el protocolo y 'val': de donde provienen los datos
  for i in range(0, len(df1)):
      sql = 'INSERT INTO `y_2004` VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
      val = (int(df1.loc[i][0]), int(df1.loc[i][1]), int(df1.loc[i][2]), int(df1.loc[i][3]), df1.loc[i][4], int(df1.loc[i][5]), 
            df1.loc[i][6], df1.loc[i][7], df1.loc[i][8], df1.loc[i][9], df1.loc[i][10], int(df1.loc[i][11]), df1.loc[i][12], 
            df1.loc[i][13], int(df1.loc[i][14]), int(df1.loc[i][15]), int(df1.loc[i][16]), int(df1.loc[i][17]), int(df1.loc[i][18]), 
            int(df1.loc[i][19]), int(df1.loc[i][20]), int(df1.loc[i][21]), int(df1.loc[i][22]), df1.loc[i][23], int(df1.loc[i][24]),
            int(df1.loc[i][25]), int(df1.loc[i][26]), int(df1.loc[i][27]), int(df1.loc[i][28]))

      mycursor.execute(sql, val)    # Se ejecuta la consulta
      mydb.commit()     # Se guardan los cambio


# Year 2005

# Se crea una funcion para cargar el set de datos a la tabla asiganada
def y_2005():
  
  # Se carga los datos del data set
  df1 = pd.read_parquet('datasets/Transforms/2005.parquet')

  # Se crea u bucle for para hacer la carga de los datos a la base de datos donde 'sql': es el protocolo y 'val': de donde provienen los datos
  for i in range(0, len(df1)):
      sql = 'INSERT INTO `y_2005` VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
      val = (int(df1.loc[i][0]), int(df1.loc[i][1]), int(df1.loc[i][2]), int(df1.loc[i][3]), df1.loc[i][4], int(df1.loc[i][5]), 
            df1.loc[i][6], df1.loc[i][7], df1.loc[i][8], df1.loc[i][9], df1.loc[i][10], int(df1.loc[i][11]), df1.loc[i][12], 
            df1.loc[i][13], int(df1.loc[i][14]), int(df1.loc[i][15]), int(df1.loc[i][16]), int(df1.loc[i][17]), int(df1.loc[i][18]), 
            int(df1.loc[i][19]), int(df1.loc[i][20]), int(df1.loc[i][21]), int(df1.loc[i][22]), df1.loc[i][23], int(df1.loc[i][24]),
            int(df1.loc[i][25]), int(df1.loc[i][26]), int(df1.loc[i][27]), int(df1.loc[i][28]))

      mycursor.execute(sql, val)    # Se ejecuta la consulta
      mydb.commit()     # Se guardan los cambio


# Year 2006

# Se crea una funcion para cargar el set de datos a la tabla asiganada
def y_2006():
  
  # Se carga los datos del data set
  df1 = pd.read_parquet('datasets/Transforms/2006.parquet')

  # Se crea u bucle for para hacer la carga de los datos a la base de datos donde 'sql': es el protocolo y 'val': de donde provienen los datos
  for i in range(0, len(df1)):
      sql = 'INSERT INTO `y_2006` VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
      val = (int(df1.loc[i][0]), int(df1.loc[i][1]), int(df1.loc[i][2]), int(df1.loc[i][3]), df1.loc[i][4], int(df1.loc[i][5]), 
            df1.loc[i][6], df1.loc[i][7], df1.loc[i][8], df1.loc[i][9], df1.loc[i][10], int(df1.loc[i][11]), df1.loc[i][12], 
            df1.loc[i][13], int(df1.loc[i][14]), int(df1.loc[i][15]), int(df1.loc[i][16]), int(df1.loc[i][17]), int(df1.loc[i][18]), 
            int(df1.loc[i][19]), int(df1.loc[i][20]), int(df1.loc[i][21]), int(df1.loc[i][22]), df1.loc[i][23], int(df1.loc[i][24]),
            int(df1.loc[i][25]), int(df1.loc[i][26]), int(df1.loc[i][27]), int(df1.loc[i][28]))

      mycursor.execute(sql, val)    # Se ejecuta la consulta
      mydb.commit()     # Se guardan los cambio


# Year 2007

# Se crea una funcion para cargar el set de datos a la tabla asiganada
def y_2007():
  
  # Se carga los datos del data set
  df1 = pd.read_parquet('datasets/Transforms/2007.parquet')

  # Se crea u bucle for para hacer la carga de los datos a la base de datos donde 'sql': es el protocolo y 'val': de donde provienen los datos
  for i in range(0, len(df1)):
      sql = 'INSERT INTO `y_2007` VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
      val = (int(df1.loc[i][0]), int(df1.loc[i][1]), int(df1.loc[i][2]), int(df1.loc[i][3]), df1.loc[i][4], int(df1.loc[i][5]), 
            df1.loc[i][6], df1.loc[i][7], df1.loc[i][8], df1.loc[i][9], df1.loc[i][10], int(df1.loc[i][11]), df1.loc[i][12], 
            df1.loc[i][13], int(df1.loc[i][14]), int(df1.loc[i][15]), int(df1.loc[i][16]), int(df1.loc[i][17]), int(df1.loc[i][18]), 
            int(df1.loc[i][19]), int(df1.loc[i][20]), int(df1.loc[i][21]), int(df1.loc[i][22]), df1.loc[i][23], int(df1.loc[i][24]),
            int(df1.loc[i][25]), int(df1.loc[i][26]), int(df1.loc[i][27]), int(df1.loc[i][28]))

      mycursor.execute(sql, val)    # Se ejecuta la consulta
      mydb.commit()     # Se guardan los cambio


# Year 2008

# Se crea una funcion para cargar el set de datos a la tabla asiganada
def y_2008():
  
  # Se carga los datos del data set
  df1 = pd.read_parquet('datasets/Transforms/2008.parquet')

  # Se crea u bucle for para hacer la carga de los datos a la base de datos donde 'sql': es el protocolo y 'val': de donde provienen los datos
  for i in range(0, len(df1)):
      sql = 'INSERT INTO `y_2008` VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
      val = (int(df1.loc[i][0]), int(df1.loc[i][1]), int(df1.loc[i][2]), int(df1.loc[i][3]), df1.loc[i][4], int(df1.loc[i][5]), 
            df1.loc[i][6], df1.loc[i][7], df1.loc[i][8], df1.loc[i][9], df1.loc[i][10], int(df1.loc[i][11]), df1.loc[i][12], 
            df1.loc[i][13], int(df1.loc[i][14]), int(df1.loc[i][15]), int(df1.loc[i][16]), int(df1.loc[i][17]), int(df1.loc[i][18]), 
            int(df1.loc[i][19]), int(df1.loc[i][20]), int(df1.loc[i][21]), int(df1.loc[i][22]), df1.loc[i][23], int(df1.loc[i][24]),
            int(df1.loc[i][25]), int(df1.loc[i][26]), int(df1.loc[i][27]), int(df1.loc[i][28]))

      mycursor.execute(sql, val)    # Se ejecuta la consulta
      mydb.commit()     # Se guardan los cambio


# Year 2015

# Se crea una funcion para cargar el set de datos a la tabla asiganada
def y_2015():
  
  # Se carga los datos del data set
  df1 = pd.read_csv('datasets/Transforms/2015.csv')

  # Se crea u bucle for para hacer la carga de los datos a la base de datos donde 'sql': es el protocolo y 'val': de donde provienen los datos
  for i in range(0, len(df1)):
      sql = 'INSERT INTO `y_2015` VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
      val = (int(df1.loc[i][0]), int(df1.loc[i][1]), int(df1.loc[i][2]), int(df1.loc[i][3]), df1.loc[i][4], int(df1.loc[i][5]), 
            df1.loc[i][6], df1.loc[i][7], df1.loc[i][8], df1.loc[i][9], df1.loc[i][10], int(df1.loc[i][11]), df1.loc[i][12], 
            df1.loc[i][13], int(df1.loc[i][14]), int(df1.loc[i][15]), df1.loc[i][16], df1.loc[i][17], int(df1.loc[i][18]), 
            int(df1.loc[i][19]), int(df1.loc[i][20]), int(df1.loc[i][21]), int(df1.loc[i][22]), int(df1.loc[i][23]), int(df1.loc[i][24]),
            df1.loc[i][25], int(df1.loc[i][26]), int(df1.loc[i][27]), int(df1.loc[i][28]), int(df1.loc[i][29]), int(df1.loc[i][30]))

      mycursor.execute(sql, val)    # Se ejecuta la consulta
      mydb.commit()     # Se guardan los cambio


# Airline

# Se crea una funcion para cargar el set de datos a la tabla asiganada
def airline():
   
   # Se carga los datos del data set
   df = pd.read_csv('datasets/Transforms/airlines.csv')

   # Se crea u bucle for para hacer la carga de los datos a la base de datos donde 'sql': es el protocolo y 'val': de donde provienen los datos
   for i in range(len(df)):
      sql = 'INSERT INTO `airline` VALUES (%s, %s)'
      val = (df.loc[i][0], df.loc[i][1])

      mycursor.execute(sql, val)    # Se ejecuta la consulta
      mydb.commit()     # Se guardan los cambio


# Airplane

# Se crea una funcion para cargar el set de datos a la tabla asiganada
def airplane():
   
   # Se carga los datos del data set
   df = pd.read_csv('datasets/Transforms/airplanes.csv')

   # Se crea u bucle for para hacer la carga de los datos a la base de datos donde 'sql': es el protocolo y 'val': de donde provienen los datos
   for i in range(len(df)):
      sql = 'INSERT INTO `airplane` VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)'
      val = (df.loc[i][0], df.loc[i][1], df.loc[i][2], df.loc[i][3], df.loc[i][4], df.loc[i][5], df.loc[i][6], 
             df.loc[i][7], int(df.loc[i][8]))

      mycursor.execute(sql, val)    # Se ejecuta la consulta
      mydb.commit()     # Se guardan los cambio


# Airport
      
# Se crea una funcion para cargar el set de datos a la tabla asiganada
def airport():
   
   # Se carga los datos del data set
   df = pd.read_csv('datasets/Transforms/airports.csv')

   # Se crea u bucle for para hacer la carga de los datos a la base de datos donde 'sql': es el protocolo y 'val': de donde provienen los datos   
   for i in range(len(df)):
      sql = 'INSERT INTO `airport` VALUES (%s, %s, %s, %s, %s, %s, %s)'
      val = (df.loc[i][0], df.loc[i][1], df.loc[i][2], df.loc[i][3], df.loc[i][4], float(df.loc[i][5]), float(df.loc[i][6]))

      mycursor.execute(sql, val)    # Se ejecuta la consulta
      mydb.commit()     # Se guardan los cambio


# Description

# Se crea una funcion para cargar el set de datos a la tabla asiganada
def description():
   
   # Se carga los datos del data set
   df = pd.read_csv('datasets/Transforms/description.csv')

   # Se crea u bucle for para hacer la carga de los datos a la base de datos donde 'sql': es el protocolo y 'val': de donde provienen los datos
   for i in range(len(df)):
      sql = 'INSERT INTO `description` VALUES (%s, %s)'
      val = (df.loc[i][0], df.loc[i][1])

      mycursor.execute(sql, val)    # Se ejecuta la consulta
      mydb.commit()     # Se guardan los cambio