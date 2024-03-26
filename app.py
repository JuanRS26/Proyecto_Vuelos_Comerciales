import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt


# ------------------------------------ Configuracion de la pagina ------------------------------------

# Se realiza la configuracion inicial de la pagina
st.set_page_config(page_title = 'Vuelos de USA', page_icon = ':world_map:', layout = 'wide')


# -------------------------------------- Carga del set de datos --------------------------------------

# Se crea una lista con los años que hay registrados para posteriormente usarlo de filtro en el apartado de 'aeropuertos'
data = ['1987', '1988', '1989', '1990', '1991', '1992', '1993', '1994', '1995', '1996', '1997', '1998', '1999', '2000', '2001',
        '2002', '2003', '2004', '2005', '2006', '2007', '2008', '2015']

# Se crea un diccionario para usarlo de firltro en el apartado de 'Registros de vuelos'
df_chart = {'Mostrar todo': None, 'Mostrar grafica de 1987': '1987', 'Mostrar grafica de 1988': '1988', 
            'Mostrar grafica de 1989': '1989','Mostrar grafica de 1990': '1990', 'Mostrar grafica de 1991': '1991', 
            'Mostrar grafica de 1992': '1992', 'Mostrar grafica de 1993': '1993','Mostrar grafica de 1994': '1994', 
            'Mostrar grafica de 1995': '1995', 'Mostrar grafica de 1996': '1996', 'Mostrar grafica de 1997': '1997',
            'Mostrar grafica de 1998': '1998', 'Mostrar grafica de 1999': '1999', 'Mostrar grafica de 2000': '2000', 
            'Mostrar grafica de 2001': '2001','Mostrar grafica de 2002': '2002', 'Mostrar grafica de 2003': '2003', 
            'Mostrar grafica de 2004': '2004', 'Mostrar grafica de 2005': '2005','Mostrar grafica de 2006': '2006', 
            'Mostrar grafica de 2007': '2007', 'Mostrar grafica de 2008': '2008', 'Mostrar grafica de 2015': '2015'}

AIRLINES = pd.read_csv('datasets/Transforms/airlines.csv')  # Se carga el set de datos de las aerolineas

# Se crea una excepción en dado caso que el set de datos 'airline_flights.csv' no este creado y asi crearlo sin que salte error
try:

    INFO_AIRLINE = pd.read_csv('datasets/Transforms/airline_flights.csv')   # Se carga el set de datos
    INFO_AIRLINE['Year'] = INFO_AIRLINE['Year'].astype(str) # se cambia el tipo de datos de la columna 'Year' para posteriormente manipularlo

except FileNotFoundError:

    st.error('Parece que hubo un error con el set de datos "airline_flights"', icon="🚨")    # Se muestra este mensaje en caso que no se haya podido cargar los datos

    # Se genera un mensaje de espera mientras se crea el set de dator requerido
    with st.spinner('Espera un poco mientras se vuelven a cargan los datos...'):

        INFO_AIRLINE = pd.DataFrame()   # Se crea un DataFrame vacio para posteriormente agregar los datos requeridos
        New_Order = ['Year', 'Airline_Code', 'Name', 'Total']   # Se crea una lista para cambiar el orden de visualizacion

        # Se crea un ciclo for para filtrar y agregar los datos necesarios de cada año en el DF que se creo vacio
        for year in data:

            df = pd.read_parquet(f'datasets/Transforms/{year}.parquet')    # Se carga el DataFrame de cada año

            df = df.groupby('Airline_Code').size().reset_index(name = 'Total')  # Se agrupa y se cuenta cada uno de los diferentes codigos de aerolinea
            df = df.sort_values(by = 'Airline_Code', ignore_index = True)   # Se ordenan de forma alfabetica
            df['Year'] = year  # Se agrega el año del cual se extrajo los datos
            
            INFO_AIRLINE = pd.concat([INFO_AIRLINE, df], ignore_index = True)   # Los datos recolectados de cada año, se agregan al DF(Info_Airline)
    
        INFO_AIRLINE['Name'] = None # Se crea una nueva columna en el DF(Info_Airline) asignandole el valor de 'None' en cada registro
        unique = pd.unique(INFO_AIRLINE['Airline_Code'])    # Se extrae los codigos de las aerolineas sin repetir

        # se crea un ciclo for para agregar el nombre de las aerolineas al DF(Info_Airline)
        for code in unique:
            # Se extrae el nombre del set de datos 'AIRLINES' filtrandolo por el codigo de aerolinea y se asigna a la
            # columna 'Name' del DF(Info_Airline) segun el codigo de la aerolinea
            INFO_AIRLINE['Name'].loc[INFO_AIRLINE['Airline_Code'] == code] = list(AIRLINES['Airline'].loc[AIRLINES['Code_IATA'] == code])[0]

        INFO_AIRLINE = INFO_AIRLINE[New_Order]  # Se ordena el DF(Info_Airline)
        INFO_AIRLINE['Year'] = INFO_AIRLINE['Year'].astype(str) # se cambia el tipo de datos de la columna 'Year' para posteriormente manipularlo

        INFO_AIRLINE.to_csv('datasets/Transforms/airline_flights.csv', index = False)   # Se guarda el set de datos en la carpeta
    
    st.success('Carga exitosa!!')   # Se muestra un mensaje al terminar el proceso de carga de los datos

# Se crea una excepción en caso que los DFs 'AIRPORT_INFO_ORIGIN' y 'AIRPORT_INFO_DESTINATION' no se encuentren en la carpeta
try:

    AIRPORT_INFO_ORIGIN = pd.read_parquet('datasets/Transforms/airport_info_origin.paquet') # Se carga el set de datos
    AIRPORT_INFO_DESTINATION = pd.read_parquet('datasets/Transforms/airport_info_destination.paquet')   # Se carga el set de datos

    # se cambia el tipo de datos de la columna 'Year' para posteriormente manipularlo
    AIRPORT_INFO_ORIGIN['Year'] = AIRPORT_INFO_ORIGIN['Year'].astype(str)
    AIRPORT_INFO_DESTINATION['Year'] = AIRPORT_INFO_DESTINATION['Year'].astype(str)

except FileNotFoundError:
    
    # Se muestra este mensaje en caso que no se haya podido cargar los datos
    st.error('Parece que hubo un error con los sets de datos "airport_info_origin" y "airport_info_destination" ', icon="🚨")

    AIRPORTS = pd.read_csv('datasets/Transforms/airports.csv')  # Se carga el set de datos 'airport.csv' para poder extraer los datos
    
    # Se crea los DFs necesarios para agregar la informacion necesaria tanto de Origen como destino
    AIRPORT_INFO_ORIGIN = pd.DataFrame()
    AIRPORT_INFO_DESTINATION = pd.DataFrame()

    # Se genera un mensaje de espera mientras se crea el set de dator requerido
    with st.spinner('Espera un poco mientras se vuelven a cargan los datos...'):
        for year in data:  #  Se crea un for para iterar y extraer la información necesaria de los sets de datos de cada año

            df = pd.read_parquet(f'datasets/Transforms/{year}.parquet') # Se carga el set de datos de cada año

            # Se crea un filtro que genera error para eliminiar las filas que tengan el codigo de aeropuerto 'CBM'
            if year == '2004':
                df = df[df['Dest_Airport'] != 'CBM']
                df = df[df['Origin_Airport'] != 'CBM']

            # Se crean dos variables auxiliares para almacenar el total de vuelos de cada aeropuerto para cada año
            aux_Origin = df.groupby('Origin_Airport').size().reset_index(name = 'Total_Flights')
            aux_Dest = df.groupby('Dest_Airport').size().reset_index(name = 'Total_Flights')

            # Se extrae el codigo de cada aeropuerto, tanto de origen como de destino sin duplicados
            Uni_Dest = pd.unique(df['Dest_Airport'])
            Uni_Origin = pd.unique(df['Origin_Airport'])

            # Se crea una  variable que almacenara los datos de los aeropuertos de origen y crea columnas nuevas
            Map_Origin = aux_Origin
            Map_Origin['Year'] = year
            Map_Origin['Name'] = None
            Map_Origin['City'] = None
            Map_Origin['State'] = None
            Map_Origin['longitude'] = None
            Map_Origin['latitude'] = None

            # Se crea una  variable que almacenara los datos de los aeropuertos de destino y crea columnas nuevas
            Map_Dest = aux_Dest
            Map_Dest['Year'] = year
            Map_Dest['Name'] = None
            Map_Dest['City'] = None
            Map_Dest['State'] = None
            Map_Dest['longitude'] = None
            Map_Dest['latitude'] = None
            
            # Se crea un for para extraer la información de cada aeropuerto de origen de 'AIRPORT' y asignarla en las columnas anteriormente creadas
            for code_origin in Uni_Origin:
                Map_Origin['Name'].loc[Map_Origin['Origin_Airport'] == code_origin] = list(AIRPORTS['Airport'].loc[AIRPORTS['Code_IATA'] == code_origin])[0]
                Map_Origin['City'].loc[Map_Origin['Origin_Airport'] == code_origin] = list(AIRPORTS['City'].loc[AIRPORTS['Code_IATA'] == code_origin])[0]
                Map_Origin['State'].loc[Map_Origin['Origin_Airport'] == code_origin] = list(AIRPORTS['State'].loc[AIRPORTS['Code_IATA'] == code_origin])[0]
                Map_Origin['longitude'].loc[Map_Origin['Origin_Airport'] == code_origin] = list(AIRPORTS['LONGITUDE'].loc[AIRPORTS['Code_IATA'] == code_origin])[0]
                Map_Origin['latitude'].loc[Map_Origin['Origin_Airport'] == code_origin] = list(AIRPORTS['LATITUDE'].loc[AIRPORTS['Code_IATA'] == code_origin])[0]

            # Se crea un for para extraer la información de cada aeropuerto de destino de 'AIRPORT' y asignarla en las columnas anteriormente creadas
            for code_dest in Uni_Dest:
                Map_Dest['Name'].loc[Map_Dest['Dest_Airport'] == code_dest] = list(AIRPORTS['Airport'].loc[AIRPORTS['Code_IATA'] == code_dest])[0]
                Map_Dest['City'].loc[Map_Dest['Dest_Airport'] == code_dest] = list(AIRPORTS['City'].loc[AIRPORTS['Code_IATA'] == code_dest])[0]
                Map_Dest['State'].loc[Map_Dest['Dest_Airport'] == code_dest] = list(AIRPORTS['State'].loc[AIRPORTS['Code_IATA'] == code_dest])[0]
                Map_Dest['longitude'].loc[Map_Dest['Dest_Airport'] == code_dest] = list(AIRPORTS['LONGITUDE'].loc[AIRPORTS['Code_IATA'] == code_dest])[0]
                Map_Dest['latitude'].loc[Map_Dest['Dest_Airport'] == code_dest] = list(AIRPORTS['LATITUDE'].loc[AIRPORTS['Code_IATA'] == code_dest])[0]
            
            # Se agrega la informacion recolectada en las variables 'Map_Origin' y 'Map_Dest' en los DFs anterior mente creados
            AIRPORT_INFO_ORIGIN = pd.concat([AIRPORT_INFO_ORIGIN, Map_Origin], ignore_index = True)
            AIRPORT_INFO_DESTINATION = pd.concat([AIRPORT_INFO_DESTINATION, Map_Dest], ignore_index = True)

        new_order = ['Year', 'Code_Airport', 'Name', 'City', 'State', 'latitude', 'longitude', 'Total_Flights'] # Se crea una lista con el nuevo orden de los DFs

        # Se renombra una de las columnas de los DFs para que sea mas intuitiva la lectura
        AIRPORT_INFO_ORIGIN.rename(columns = {'Origin_Airport': 'Code_Airport'}, inplace = True)
        AIRPORT_INFO_DESTINATION.rename(columns = {'Dest_Airport': 'Code_Airport'}, inplace = True)

        # Se organiza las columnas de los DFs
        AIRPORT_INFO_ORIGIN = AIRPORT_INFO_ORIGIN[new_order]
        AIRPORT_INFO_DESTINATION = AIRPORT_INFO_DESTINATION[new_order]

        # Se guarda los sets de datos en la carpeta
        AIRPORT_INFO_ORIGIN.to_parquet('datasets/Transforms/airport_info_origin.paquet', index = False)
        AIRPORT_INFO_DESTINATION.to_parquet('datasets/Transforms/airport_info_destination.paquet', index = False)
    
    st.success('Carga exitosa!!')   # Se muestra un mensaje al terminar el proceso de carga de los datos


# -------------------------------------- Titulo de la pagina --------------------------------------

# Se crea el titulo con sus parametros
st.title(
    ':airplane_departure: VUELOS COMERCIALES U.S.A',
    help = 'Esto es un Dashborad acerca de los vuelos comerciales en Estado Unidos \
            entre los años 1987 hasta 2008 y un registro del año 2015',
    anchor = False
)


# -------------------------------------- Contenido de la pagina --------------------------------------

# Se crea la division de la pagina es tabs
tab1, tab2, tab3 = st.tabs([
                                  ':clipboard: Registros de vuelos', 
                                  ':chart_with_upwards_trend: Aerolineas',
                                  ':world_map: Aeropuertos'
                                ])

# Se agrega la informacion para el tab 1 'Registros de vuelos'
with tab1:
    
    Total_Count = pd.DataFrame()    # Se crea un DataFrame para agregar la información que se requiera
    Unique_Code = pd.unique(INFO_AIRLINE['Airline_Code'])   # Se extrae el codigo de las aerolineas

    # Se crea un for para extraer el registro total de los vuelos de cada aerolinea 
    for code in Unique_Code:
        aux_sum = INFO_AIRLINE['Total'].loc[INFO_AIRLINE['Airline_Code'] == code].sum() # Se suma el total de los registros de vuelos de cada año por aerolinea
        aux_name = list(AIRLINES['Airline'].loc[AIRLINES['Code_IATA'] == code]) # Se extrae el nombre de la aerolinea 
        # Se crea un DF auxiliar para almacenar la informacion y poder concatenarla posteriormente
        df_aux = pd.DataFrame({'Airline_Code': [code], 'Name': aux_name, 'Total': [aux_sum]})

        Total_Count = pd.concat([Total_Count, df_aux], ignore_index = True) # Se guarda la información recolectada en el DF(Total_Count)

    # Se crea un encavezado con sus parametros
    st.header(
        'REGISTROS DE VUELOS',
        help = 'Aqui se mostrara el total de registros de los vuelos y el total de vuelos por aerolinea',
        anchor = False,
        divider = 'grey'
    )

    Select_Year = st.selectbox('Seleccione un año', list(df_chart.keys()))  # Se crea una lista desplegable con los años a filtrar
    Year = INFO_AIRLINE[INFO_AIRLINE['Year'] == df_chart.get(Select_Year)]  # Se extrae el año que el usuario selecciono de la lista
        
    # Se crea un filtro por si el usuario quiere ver los datos del total de registros de todos los años
    if Select_Year == 'Mostrar todo':

        Total_Count = Total_Count.sort_values(by = 'Airline_Code', ignore_index = True) # Se ordena el DF alfabeticamente por codigo
        Sum_Total = Total_Count['Total'].sum()  # Se extrae la suma total de todos los vuelos registrados para todos los años
        Sum_Total = '{:,.0f}'.format(Sum_Total).replace(',', '.')   # Se arregla el numero total para que tenga los signos de puntuacion

        # Se crea un sub-encabezado con sus parametros para mostrar la suma total de vuelos registrados
        st.subheader(
            f'Las graficas registran un total de vuelos de {Sum_Total} para todos los años',
            help = f'Estas grafica de barras muestra el total de vuelos registrados por cada una de las aerolineas para el año "{Select_Year}"',
            divider = 'violet',
            anchor = False
        )

        col1, col2 = st.columns(2)  # Se crean dos columnas para acomodar las graficas mejor

        with col1: # se agrega la grafica en la primera columna
            # Se crea una variable con el DF ordenado de mayor a menor
            Info_Chart_Bar = Total_Count.sort_values(by = 'Total', ascending = False, ignore_index = True) 
            Info_Chart_Bar = Info_Chart_Bar.head(5) # Se extrae las 5 aerolineas con mayor registro de vuelos

            y = Info_Chart_Bar['Total'] # Se crea una variable que almacena los registros de cada aerolinea
            mylabels = Info_Chart_Bar['Name']   # Se crea una variable que contiene los nombres de las aerolineas
            myexplode = [0.2, 0, 0, 0, 0]   # Se crea la forma en que se visualiza el grafico
            # Se extrae la información para identificar las aerolineas con mayor registro
            label_legend = list(Info_Chart_Bar['Airline_Code'] + ' -> ' + Info_Chart_Bar['Total'].astype(str))

            fig, ax = plt.subplots(figsize = [8,8]) # Se genera la figura asignando el tamaño como parametro

            ax.pie(y, labels = mylabels, explode = myexplode, shadow = True)    # Se asigna el tipo de grafica con los parametros
            ax.legend(label_legend, shadow = True, loc = 'upper center', fontsize = 'large')    # Se modifica la visualizacion de la leyenda

            st.pyplot(fig)  # Se genera la grafica en la pagina

        with col2:  # Se agrega la grafica en la segunda columna
            # Se genera la segunda grafica que es de tipo barra con los paramentros requeridos
            st.bar_chart(Total_Count, x = 'Airline_Code', y = 'Total', height = 600, color = '#78288C')

    # Si el usuario quiere ver los datos por año, se mostrara lo siguiente
    else:
        # Se extrae el total de los registros de vuelos de cada aerolinea para el año seleccionado
        Sum_Total = INFO_AIRLINE[INFO_AIRLINE['Year'] == df_chart.get(Select_Year)]
        Sum_Total = Sum_Total['Total'].sum() # Se suma todos los registros de vuelos de el año seleccionado
        Sum_Total = '{:,.0f}'.format(Sum_Total).replace(',', '.') # Se arregla el numero total para que tenga los signos de puntuacion

        # Se crea un sub-encabezado con sus parametros para mostrar la suma total de vuelos registrados para el año seleccionado
        st.subheader(
            f'La grafica registra un total de vuelos de {Sum_Total} para el año {df_chart.get(Select_Year)}',
            help = f'Esta grafica de barras muestra el total de vuelos registrados por cada una de las aerolineas para el año "{Select_Year}"',
            divider = 'violet',
            anchor = False
        )

        st.bar_chart(Year, x = 'Airline_Code', y = 'Total', color = '#78288C') # Se genera la grafica de barras con los parametros requeridos

    # Se crea un sub-encabezado para informar detalles del año seleccionado
    st.subheader(
        'Detalles de las aerolineas',
        help = f'Esta tabla muestra mas a detalle la informacion de cada una de las aerolineas registradas para el año "{Select_Year}"',
        divider = 'violet',
        anchor = False
    )

    with st.expander('Desplegar lista'):    # La función 'st.expander()' ayuda a desplegar la lista con la información
        
        # Se hace un filtro en dado caso que el usuario quiera ver todos los años
        if Select_Year == 'Mostrar todo':
            st.dataframe(Total_Count, use_container_width = True, height = 700) # Se muetra toda la informacion de los registros de vuelos

        else:
            st.dataframe(Year.reset_index(drop = True), use_container_width = True, height = 700) # Se muetra toda la información para el año seleccionado

# Se agrega la informacion para el tab 2 'Aerolineas'
with tab2:  

    unique = pd.unique(INFO_AIRLINE['Airline_Code'])    # Se extrae el codigo de las aerolineas
    dictionary_Select = {}  # Se crea un diccionario para mostrar mejor la lista de opciones

    # Se crea un for para guardar los nombres y codigos de las aerolineas
    for code in unique:
        dictionary_Select['(' + code + ') ' + list(AIRLINES['Airline'].loc[AIRLINES['Code_IATA'] == code])[0]] = code

    # Se crea un encabezado con sus parametros
    st.header(
        'AEROLINEAS',
        help = 'Aqui se mostrara toda la informacion relacionada unicamente con las aerolineas',
        divider = 'violet',
        anchor = False
    )
    
    # Se crea una lista para seleccionar una aerolinea
    Select_Airline = st.selectbox('Seleccione la grafica de la aerolinea', list(sorted(dictionary_Select.keys())))

    Code_Airline = dictionary_Select.get(Select_Airline)    # Se extrae el codigo de la aerolinea de la opción que selecciono el usuario

    # Se extrae la informacion de la aerolinea seleccionada
    Data_Airline = INFO_AIRLINE[INFO_AIRLINE['Airline_Code'] == Code_Airline]

    Sum_Total = Data_Airline['Total'].sum() # Se realiza la suma del total de vuelos reallizados por la aerolinea
    Sum_Total = '{:,.0f}'.format(Sum_Total).replace(',', '.') # Se arregla el numero total para que tenga los signos de puntuacion

    # Se crea un sub-encabezado con los parametros dando información de la grafica
    st.subheader(
        f'La grafica muestra un total de {Sum_Total} vuelos registrados',
        help = f'La siguiente grafica muestra la cantidad de vuelos por año que ha tenido la aerolinea {Select_Airline}',
        divider = 'violet',
        anchor = False
    )

    st.line_chart(Data_Airline, x = 'Year', y = 'Total', color = '#78288C') # Se genera la grafica de linea con sus parametros

    # se crea un sub-encabezado para informar acerca de la lista desplegable
    st.subheader(
        f'Detalles de la aerolinea {Select_Airline}',
        help = 'La siguente tabla mostrara a detalle cada uno de los años y la cantidad de vuelos que hubieron',
        divider = 'violet',
        anchor = False
    )

    with st.expander('Desplegar lista'):    # La función 'st.expander()' ayuda a desplegar la lista con la información
        st.dataframe(Data_Airline[['Year', 'Total']].reset_index(drop = True), use_container_width = True, height = 700,)


# Se agrega la informacion para el tab 3 'Aeropuertos'
with tab3:

    # Se crea un encabezado con sus parametros
    st.header(
        'AEROPUERTOS',
        help = 'Aqui se mostrara toda la informaciòn de los aeropuertos segun el año y los registros de salida del vuelo (origen) \
                ò llegada del vuelo (Destino)',
        divider = 'violet', 
        anchor = False
    )

    # Se crea una lista con cada uno de los años
    Year_Option = st.selectbox('Selecciona un año', data)
    # Se crea la opción para que el usuario filtr si desea ver los datos de los aeropuertos de origen o destino
    Map_Option = st.radio('Seleccione entre vuelos de **Origen** o vuelos de **Destino**', ['Origen', 'Destino'], horizontal = True)

    # Se crea un sub-encabezado con sus parametros dando informacion de las graficas 
    st.subheader(
        f'Grafica del Top de vuelos y mapa de los aeropuertos para el año {Year_Option}', 
        help = f'La grafica muestra el top 5 de los aeropuertos que tienen mas registros de vuelos \
                 y el mapa muestra la ubicacion de cada uno de los aeropuertos para el año {Year_Option}',
        divider = 'violet',
        anchor = False
    )

    col1, col2 = st.columns(2)  # Se crean dos columnas para acomodar las graficas mejor

    with col1:  # se agrega la grafica en la primera columna
        # Se crea un filtro para mostrar la grafica de origen o de destino
        if Map_Option == 'Origen':

            # Se crea una variable con el DataFrame organizado de mayor a menor segun el registro de vuelos
            Top_Origin = AIRPORT_INFO_ORIGIN.sort_values(by = 'Total_Flights', ascending = False, ignore_index = True)
            Top_Origin = Top_Origin.loc[Top_Origin['Year'] == Year_Option].head(5)  # Se extrae los 5 primeros aeropuertos

            y = Top_Origin['Total_Flights'] # Se crea una variable que almacena los registros de cada aeropuerto
            mylabels = Top_Origin['Name']   # Se crea una variable que contiene los nombres de los aeropuertos
            myexplode = [0.2, 0, 0, 0, 0]   # Se crea la forma en que se visualiza el grafico
            # Se extrae la información para identificar los aeropuertos con mayor registros
            label_legend = list(Top_Origin['Code_Airport'] + ' -> ' + Top_Origin['Total_Flights'].astype(str))

            fig, ax = plt.subplots(figsize = [8,8]) # Se genera la figura asignando el tamaño como parametro

            ax.pie(y, labels = mylabels, explode = myexplode, shadow = True)    # Se asigna el tipo de grafica con los parametros
            ax.legend(label_legend, shadow = True, loc = 'upper center', fontsize = 'large')    # Se modifica la visualizacion de la leyenda

            st.pyplot(fig)  # Se genera la grafica en la pagina

        elif Map_Option == 'Destino':

            # Se crea una variable con el DataFrame organizado de mayor a menor segun el registro de vuelos
            Top_Dest = AIRPORT_INFO_DESTINATION.sort_values(by = 'Total_Flights', ascending = False, ignore_index = True)
            Top_Dest = Top_Dest.loc[Top_Dest['Year'] == Year_Option].head(5)    # Se extrae los 5 primeros aeropuertos

            y = Top_Dest['Total_Flights']   # Se crea una variable que almacena los registros de cada aeropuerto
            mylabels = Top_Dest['Name']     # Se crea una variable que contiene los nombres de los aeropuertos
            myexplode = [0.2, 0, 0, 0, 0]   # Se crea la forma en que se visualiza el grafico
            # Se extrae la información para identificar los aeropuertos con mayor registros
            label_legend = list(Top_Dest['Code_Airport'] + ' -> ' + Top_Dest['Total_Flights'].astype(str))

            fig, ax = plt.subplots(figsize = [8,8]) # Se genera la figura asignando el tamaño como parametro

            ax.pie(y, labels = mylabels, explode = myexplode, shadow = True)    # Se asigna el tipo de grafica con los parametros
            ax.legend(label_legend, shadow = True, loc = 'upper center', fontsize = 'large')    # Se modifica la visualizacion de la leyenda

            st.pyplot(fig)  # Se genera la grafica en la pagina

    with col2:  # se agrega la grafica en la segunda columna
        # Se crea un filtro para mostrar el mapa con sus parametros tanto de origen como de destino
        if Map_Option == 'Origen':
            st.map(AIRPORT_INFO_ORIGIN.loc[AIRPORT_INFO_ORIGIN['Year'] == Year_Option], latitude = 'latitude', longitude = 'longitude')

        elif Map_Option == 'Destino':
            st.map(AIRPORT_INFO_DESTINATION.loc[AIRPORT_INFO_DESTINATION['Year'] == Year_Option], latitude = 'latitude', longitude = 'longitude') 

    # Se crea un sub-encabezado para dar informacion e indicar la lista desplegable
    st.subheader(
        f'Lista de aeropuertos para el año {Year_Option}', 
        help = f'Se muetra la lista completa de los aeropuertos con informaciòn mas detallada para el año {Year_Option}',
        divider = 'violet',
        anchor = False
    )


    with st.expander('Desplegar lista'):    # La función 'st.expander()' ayuda a desplegar la lista con la información
        # Se realiza un filtro para mostrar la información de los aeropuertos de origen o de destino
        if Map_Option == 'Origen':
            # Se genera la tabla con todos los datos informativos acerca de los aeropuerto para origen
            st.dataframe(
                AIRPORT_INFO_ORIGIN.loc[AIRPORT_INFO_ORIGIN['Year'] == Year_Option].reset_index(drop = True),
                use_container_width = True,
                height = 700
            )

        elif Map_Option == 'Destino':
            # Se genera la tabla con todos los datos informativos acerca de los aeropuerto para destino
            st.dataframe(
                AIRPORT_INFO_DESTINATION.loc[AIRPORT_INFO_DESTINATION['Year'] == Year_Option].reset_index(drop = True),
                use_container_width = True,
                height = 700
            )