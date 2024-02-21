import pandas as pd
import numpy as np
import streamlit as st
import matplotlib.pyplot as plt


st.set_page_config(page_title = 'Vuelos de USA', page_icon = ':world_map:', layout = 'wide')

data = ['1987', '1988', '1989', '1990', '1991', '1992', '1993', '1994', '1995', '1996', '1997', '1998', '1999', '2000', '2001',
        '2002', '2003', '2004', '2005', '2006', '2007', '2008', '2015']

airlines = pd.read_csv('datasets/Transforms/airlines.csv')
airport = pd.read_csv('datasets/Transforms/airports.csv')

AIRPORT_INFO_ORIGIN = pd.DataFrame()
AIRPORT_INFO_DESTINATION = pd.DataFrame()

try:

    Info_Airline = pd.read_csv('datasets/Transforms/airline_flights.csv')
    Info_Airline['Year'] = Info_Airline['Year'].astype(str)

except FileNotFoundError:

    st.error('Parece que hubo un error con un set de datos ', icon="🚨")

    with st.spinner('Espera un poco mientras se vuelven a cargan los datos...'):

        Info_Airline = pd.DataFrame()
        New_Order = ['Year', 'Airline_Code', 'Name', 'Total']

        for i in data:

            df = pd.read_parquet(f'datasets/Transforms/{i}.parquet')

            df = df.groupby('Airline_Code').size().reset_index(name = 'Total')
            df = df.sort_values(by = 'Airline_Code', ignore_index = True)
            df['Year'] = i
            
            Info_Airline = pd.concat([Info_Airline, df], ignore_index = True)
    
        Info_Airline['Name'] = None
        unique = pd.unique(Info_Airline['Airline_Code'])

        for j in unique:
            Info_Airline['Name'].loc[Info_Airline['Airline_Code'] == j] = list(airlines['Airline'].loc[airlines['Code_IATA'] == j])[0]

        Info_Airline = Info_Airline[New_Order]
        Info_Airline['Year'] = Info_Airline['Year'].astype(str)

        Info_Airline.to_csv('datasets/Transforms/airline_flights.csv', index = False)
    
    st.success('Carga exitosa!!')

try:

    AIRPORT_INFO_ORIGIN = pd.read_parquet('datasets/Transforms/airport_info_origin.paquet')
    AIRPORT_INFO_DESTINATION = pd.read_parquet('datasets/Transforms/airport_info_destination.paquet')

    AIRPORT_INFO_ORIGIN['Year'] = AIRPORT_INFO_ORIGIN['Year'].astype(str)
    AIRPORT_INFO_DESTINATION['Year'] = AIRPORT_INFO_DESTINATION['Year'].astype(str)

except FileNotFoundError:

    st.error('Parece que hubo un error con un set de datos ', icon="🚨")

    with st.spinner('Espera un poco mientras se vuelven a cargan los datos...'):
        for j in data:

            df = pd.read_parquet(f'datasets/Transforms/{j}.parquet')

            if j == '2004':
                df = df[df['Dest_Airport'] != 'CBM']
                df = df[df['Origin_Airport'] != 'CBM']

            aux_Origin = df.groupby('Origin_Airport').size().reset_index(name = 'Total_Flights')
            aux_Dest = df.groupby('Dest_Airport').size().reset_index(name = 'Total_Flights')

            Uni_Dest = pd.unique(df['Dest_Airport'])
            Uni_Origin = pd.unique(df['Origin_Airport'])

            Map_Origin = aux_Origin
            Map_Origin['Year'] = j
            Map_Origin['Name'] = None
            Map_Origin['City'] = None
            Map_Origin['State'] = None
            Map_Origin['longitude'] = None
            Map_Origin['latitude'] = None

            Map_Dest = aux_Dest
            Map_Dest['Year'] = j
            Map_Dest['Name'] = None
            Map_Dest['City'] = None
            Map_Dest['State'] = None
            Map_Dest['longitude'] = None
            Map_Dest['latitude'] = None

            for i in Uni_Origin:
                Map_Origin['Name'].loc[Map_Origin['Origin_Airport'] == i] = list(airport['Airport'].loc[airport['Code_IATA'] == i])[0]
                Map_Origin['City'].loc[Map_Origin['Origin_Airport'] == i] = list(airport['City'].loc[airport['Code_IATA'] == i])[0]
                Map_Origin['State'].loc[Map_Origin['Origin_Airport'] == i] = list(airport['State'].loc[airport['Code_IATA'] == i])[0]
                Map_Origin['longitude'].loc[Map_Origin['Origin_Airport'] == i] = list(airport['LONGITUDE'].loc[airport['Code_IATA'] == i])[0]
                Map_Origin['latitude'].loc[Map_Origin['Origin_Airport'] == i] = list(airport['LATITUDE'].loc[airport['Code_IATA'] == i])[0]

            for k in Uni_Dest:

                Map_Dest['Name'].loc[Map_Dest['Dest_Airport'] == k] = list(airport['Airport'].loc[airport['Code_IATA'] == k])[0]
                Map_Dest['City'].loc[Map_Dest['Dest_Airport'] == k] = list(airport['City'].loc[airport['Code_IATA'] == k])[0]
                Map_Dest['State'].loc[Map_Dest['Dest_Airport'] == k] = list(airport['State'].loc[airport['Code_IATA'] == k])[0]
                Map_Dest['longitude'].loc[Map_Dest['Dest_Airport'] == k] = list(airport['LONGITUDE'].loc[airport['Code_IATA'] == k])[0]
                Map_Dest['latitude'].loc[Map_Dest['Dest_Airport'] == k] = list(airport['LATITUDE'].loc[airport['Code_IATA'] == k])[0]

            AIRPORT_INFO_ORIGIN = pd.concat([AIRPORT_INFO_ORIGIN, Map_Origin], ignore_index = True)
            AIRPORT_INFO_DESTINATION = pd.concat([AIRPORT_INFO_DESTINATION, Map_Dest], ignore_index = True)


        new_order = ['Year', 'Code_Airport', 'Name', 'City', 'State', 'latitude', 'longitude', 'Total_Flights']

        AIRPORT_INFO_ORIGIN.rename(columns = {'Origin_Airport': 'Code_Airport'}, inplace = True)
        AIRPORT_INFO_DESTINATION.rename(columns = {'Dest_Airport': 'Code_Airport'}, inplace = True)

        AIRPORT_INFO_ORIGIN = AIRPORT_INFO_ORIGIN[new_order]
        AIRPORT_INFO_DESTINATION = AIRPORT_INFO_DESTINATION[new_order]

        AIRPORT_INFO_ORIGIN.to_parquet('datasets/Transforms/airport_info_origin.paquet', index = False)
        AIRPORT_INFO_DESTINATION.to_parquet('datasets/Transforms/airport_info_destination.paquet', index = False)
    
    st.success('Carga exitosa!!')

# --------------------------------------------------------------------------------------------------------------------------------


df_chart = {'Mostrar todo': None, 'Show Graphic 1987': '1987', 'Show Graphic 1988': '1988', 'Show Graphic 1989': '1989', 
            'Show Graphic 1990': '1990', 'Show Graphic 1991': '1991', 'Show Graphic 1992': '1992', 'Show Graphic 1993': '1993', 
            'Show Graphic 1994': '1994', 'Show Graphic 1995': '1995', 'Show Graphic 1996': '1996', 'Show Graphic 1997': '1997', 
            'Show Graphic 1998': '1998', 'Show Graphic 1999': '1999', 'Show Graphic 2000': '2000', 'Show Graphic 2001': '2001', 
            'Show Graphic 2002': '2002', 'Show Graphic 2003': '2003', 'Show Graphic 2004': '2004', 'Show Graphic 2005': '2005', 
            'Show Graphic 2006': '2006', 'Show Graphic 2007': '2007', 'Show Graphic 2008': '2008', 'Show Graphic 2015': '2015'}

Unique_Airlines = list(pd.unique(Info_Airline['Airline_Code']))

# -----------------------------------------------------------------------------------------------------------
Primary_Title = ':airplane_departure: VUELOS COMERCIALES U.S.A' 

st.title(f'{Primary_Title}', help = 'Esto es un Dashborad acerca de los vuelos comerciales en Estado Unidos \
         entre los años 1987 hasta 2008 y un registro del año 2015',
         anchor = False)

# -----------------------------------------------------------------------------------------------------------


# Pruebas
# tab1, tab3, tab4, tab5 = st.tabs([':clipboard: Registros de vuelos', ':chart_with_upwards_trend: Aerolineas',
#                                   ':world_map: Aeropuertos', ':bomb: Pruebas'])

tab1, tab3, tab4 = st.tabs([':clipboard: Registros de vuelos', ':chart_with_upwards_trend: Aerolineas',
                                  ':world_map: Aeropuertos'])


with tab1:
    
    Total_Count = pd.DataFrame()
    Unique_Code = pd.unique(Info_Airline['Airline_Code'])

    for code in Unique_Code:
        aux_sum = Info_Airline['Total'].loc[Info_Airline['Airline_Code'] == code].sum()
        aux_name = list(airlines['Airline'].loc[airlines['Code_IATA'] == code])
        df_aux = pd.DataFrame({'Airline_Code': [code], 'Name': aux_name, 'Total': [aux_sum]})

        Total_Count = pd.concat([Total_Count, df_aux], ignore_index = True)

    Total_Count = Total_Count.sort_values(by = 'Airline_Code', ignore_index = True)
    Sum_Total = Total_Count['Total'].sum()
    Sum_Total = '{:,.0f}'.format(Sum_Total).replace(',', '.')

    st.header(
        f'HAY UN TOTAL DE {Sum_Total} REGISTROS DE VUELOS',
        help = 'Aqui se mostrara el total de registros de los vuelos y el total de vuelos por aerolinea',
        anchor = False,
        divider = 'grey'
    )

    Select_Year = st.selectbox('Seleccione un año', list(df_chart.keys()))
    
    st.subheader(
        'Grafica de registro de vuelos totales por aerolinea',
        help = f'Esta grafica de barras muestra el total de vuelos registrados por cada una de las aerolineas para el año "{Select_Year}"',
        divider = 'violet',
        anchor = False
    )

    if Select_Year == 'Mostrar todo':


        col1, col2 = st.columns(2)

        with col1:
            Info_Chart_Bar = Total_Count.sort_values(by = 'Total', ascending = False, ignore_index = True)
            Info_Chart_Bar = Info_Chart_Bar.head(5)

            y = Info_Chart_Bar['Total']
            mylabels = Info_Chart_Bar['Name']
            myexplode = [0.2, 0, 0, 0, 0]
            label_legend = list(Info_Chart_Bar['Airline_Code'] + ' -> ' + Info_Chart_Bar['Total'].astype(str))

            fig, ax = plt.subplots(figsize = [8,8])

            ax.pie(y, labels = mylabels, explode = myexplode, shadow = True)
            ax.legend(label_legend, shadow = True, loc = 'upper center', fontsize = 'large')

            st.pyplot(fig)

        with col2:
            st.bar_chart(Total_Count, x = 'Airline_Code', y = 'Total', height = 600, color = '#78288C')

    else:
        Year = Info_Airline[Info_Airline['Year'] == df_chart.get(Select_Year)]
        st.bar_chart(Year, x = 'Airline_Code', y = 'Total', color = '#78288C')

    st.subheader(
        'Detalles de las aerolineas',
        help = f'Esta tabla muestra mas a detalle la informacion de cada una de las aerolineas registradas para el año "{Select_Year}"',
        divider = 'violet',
        anchor = False
    )

    with st.expander('Desplegar lista'):

        if Select_Year == 'Mostrar todo':
            st.dataframe(Total_Count, use_container_width = True, height = 700)

        else:
            Year = Info_Airline[Info_Airline['Year'] == df_chart.get(Select_Year)]
            st.dataframe(Year.reset_index(drop = True), use_container_width = True, height = 700)

with tab3:  

    unique = pd.unique(Info_Airline['Airline_Code'])
    dictionary_Select = {}

    for i in unique:
        dictionary_Select['(' + i + ') ' + list(airlines['Airline'].loc[airlines['Code_IATA'] == i])[0]] = i

    st.header(
        'AEROLINEAS',
        help = 'Aqui se mostrara toda la informacion relacionada unicamente con las aerolineas',
        divider = 'violet',
        anchor = False
    )
    
    Select_Airline = st.selectbox('Seleccione la grafica de la aerolinea', list(sorted(dictionary_Select.keys())))

    Code_Airline = dictionary_Select.get(Select_Airline)

    Data_Airline_Year = Info_Airline[Info_Airline['Airline_Code'] == Code_Airline]

    Sum_Total = Data_Airline_Year['Total'].sum()
    Sum_Total = '{:,.0f}'.format(Sum_Total).replace(',', '.')

    st.subheader(
        f'La grafica muestra un total de {Sum_Total} vuelos registrados',
        help = f'La siguiente grafica muestra la cantidad de vuelos por año que ha tenido la aerolinea {Select_Airline}',
        divider = 'violet',
        anchor = False
    )

    st.line_chart(Data_Airline_Year, x = 'Year', y = 'Total', color = '#78288C')

    st.subheader(
        f'Detalles de la aerolinea {Select_Airline}',
        help = 'La siguente tabla mostrara a detalle cada uno de los años y la cantidad de vuelos que hubieron',
        divider = 'violet',
        anchor = False
    )

    with st.expander('Desplegar lista'):
        st.dataframe(Data_Airline_Year[['Year', 'Total']].reset_index(drop = True), use_container_width = True, height = 700,)


with tab4:

    st.header(
        'AEROPUERTOS',
        help = 'Aqui se mostrara toda la informaciòn de los aeropuertos segun el año y los registros de salida del vuelo (origen) \
                ò llegada del vuelo (Destino)',
        divider = 'violet', 
        anchor = False
    )

    Year_Option = st.selectbox('Selecciona un año', data)
    Map_Option = st.radio('Seleccione entre vuelos de **Origen** o vuelos de **Destino**', ['Origen', 'Destino'], horizontal = True)

    st.subheader(
        f'Grafica del Top de vuelos y mapa de los aeropuertos para el año {Year_Option}', 
        help = f'La grafica muestra el top 5 de los aeropuertos que tienen mas registros de vuelos \
                 y el mapa muestra la ubicacion de cada uno de los aeropuertos para el año {Year_Option}',
        divider = 'violet',
        anchor = False
    )

    col1, col2 = st.columns(2)

    with col1:
        if Map_Option == 'Origen':

            Top_Origin = AIRPORT_INFO_ORIGIN.sort_values(by = 'Total_Flights', ascending = False, ignore_index = True)
            Top_Origin = Top_Origin.loc[Top_Origin['Year'] == Year_Option].head(5)

            y = Top_Origin['Total_Flights']
            mylabels = Top_Origin['Name']
            myexplode = [0.2, 0, 0, 0, 0]
            label_legend = list(Top_Origin['Code_Airport'] + ' -> ' + Top_Origin['Total_Flights'].astype(str))

            fig, ax = plt.subplots(figsize = [8,8])

            ax.pie(y, labels = mylabels, explode = myexplode, shadow = True)
            ax.legend(label_legend, shadow = True, loc = 'upper center', fontsize = 'large')

            st.pyplot(fig)

        elif Map_Option == 'Destino':

            Top_Dest = AIRPORT_INFO_DESTINATION.sort_values(by = 'Total_Flights', ascending = False, ignore_index = True)
            Top_Dest = Top_Dest.loc[Top_Dest['Year'] == Year_Option].head(5)

            y = Top_Dest['Total_Flights']
            mylabels = Top_Dest['Name']
            myexplode = [0.2, 0, 0, 0, 0]
            label_legend = list(Top_Dest['Code_Airport'] + ' -> ' + Top_Dest['Total_Flights'].astype(str))

            fig, ax = plt.subplots(figsize = [8,8])

            ax.pie(y, labels = mylabels, explode = myexplode, shadow = True)
            ax.legend(label_legend, shadow = True, loc = 'upper center', fontsize = 'large')

            st.pyplot(fig)  

    with col2:

        if Map_Option == 'Origen':
            st.map(AIRPORT_INFO_ORIGIN.loc[AIRPORT_INFO_ORIGIN['Year'] == Year_Option], latitude = 'latitude', longitude = 'longitude')

        elif Map_Option == 'Destino':
            st.map(AIRPORT_INFO_DESTINATION.loc[AIRPORT_INFO_DESTINATION['Year'] == Year_Option], latitude = 'latitude', longitude = 'longitude') 

    st.subheader(
        f'Lista de aeropuertos para el año {Year_Option}', 
        help = f'Se muetra la lista completa de los aeropuertos con informaciòn mas detallada para el año {Year_Option}',
        divider = 'violet',
        anchor = False
    )

    with st.expander('Desplegar lista'):
        if Map_Option == 'Origen':
            st.dataframe(
                AIRPORT_INFO_ORIGIN.loc[AIRPORT_INFO_ORIGIN['Year'] == Year_Option].reset_index(drop = True),
                use_container_width = True,
                height = 700
            )

        elif Map_Option == 'Destino':
            st.dataframe(
                AIRPORT_INFO_DESTINATION.loc[AIRPORT_INFO_DESTINATION['Year'] == Year_Option].reset_index(drop = True),
                use_container_width = True,
                height = 700
            )
            
# with tab5:

#     st.header('**Esto** es un *encabezado* con division', divider = 'green')
#     st.subheader('**Esto** es un *sub-encabezado* con division', divider = 'violet')
#     st.divider()

#     a = st.button('Boton 1')
#     b = st.button('Boton 2')
#     c = st.button('Boton 3')
#     d = st.button('Boton 4')
#     e = st.button('Boton 5')


#     st.write(f'El boton 1 esta en estado: {a}')
#     st.write(f'El boton 2 esta en estado: {b}')
#     st.write(f'El boton 3 esta en estado: {c}')