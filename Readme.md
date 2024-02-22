# <h1 align="center">**`VUELOS COMERCIALES`**</h1>

<p align="center">
<img src="https://images-wixmp-ed30a86b8c4ca887773594c2.wixmp.com/f/bc0a4715-c860-464f-88e4-3045f9106b4c/d8kgg6n-59fc17d1-aad2-47f0-b036-2a7c522dd403.png?token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ1cm46YXBwOjdlMGQxODg5ODIyNjQzNzNhNWYwZDQxNWVhMGQyNmUwIiwiaXNzIjoidXJuOmFwcDo3ZTBkMTg4OTgyMjY0MzczYTVmMGQ0MTVlYTBkMjZlMCIsIm9iaiI6W1t7InBhdGgiOiJcL2ZcL2JjMGE0NzE1LWM4NjAtNDY0Zi04OGU0LTMwNDVmOTEwNmI0Y1wvZDhrZ2c2bi01OWZjMTdkMS1hYWQyLTQ3ZjAtYjAzNi0yYTdjNTIyZGQ0MDMucG5nIn1dXSwiYXVkIjpbInVybjpzZXJ2aWNlOmZpbGUuZG93bmxvYWQiXX0.G6xjMhhhbjE4SigACvtsuhQDCWDfAMHqSFvVTdQl8mk"   
height="400">
</p>

## **Contexto**

La aviación comercial, o bien aviación de transporte regular comercial, es la actividad principal que desempeñan las pequeñas o grandes compañías aéreas, dedicándose al transporte aéreo de personas o mercancías siguiendo cierta regularidad. 

Dentro de los últimos 10 años, los vuelos comerciales han experimentado un amplio proceso de renovación, instaurando medidas de reducción de costos de explotación, eliminación de rutas no rentables y renovación de la tecnología propia de sus aviones, permitiendo tener una flota más eficiente en cuanto a combustible y emisiones.

## **Objetivos futuros**

- **Definir KPI'S**, tales como reducir la cancelación de vuelo en un 10% en cierto período de tiempo, disminuir o aumentar la flota para ocupar cierto porcentaje de mercado, reducir en cierto porcentaje los destinos poco frecuentes, etc.

- **Generar mapas** sobre concentración de vuelos, destinos frecuentes, rutas frecuentes, etc.

## **Quick start**
- ### Activacion de la app

1. Descarga La carpeta con los sets de datos y agregala en el proyecto: [Datasets Kaggle](https://www.kaggle.com/datasets/juanrs26/vuelos-comerciales/data)

2. Instala las dependencias necesarias:
```python
pip install -r requirements.txt
```

3. Inicia la app para poder interactuar con el dashboard:
```python
streamlit run app.py
```

- ### Actualización/Carga de datos

1.Si tienes un gestor de bases de datos MySQL y deseas conectarla, primero debes ejecutar en tu gestor de base de datos el **script** que se encuentra en la carpeta 'database/scripts' y crear la base de datos para realizar la conexión. 

2. Inicia el menu el la linea de comandos utilizando:
```python
py main.py
```

3. Ingrea la información de tu base de datos para realizar la conexión:
![Imagen de conexión a la base de datos](images/info_database.png)
Posteriormente se guardaran los datos y no sera necesario volver a ingresarlos

## **Principales fuentes**

- 2015 Flight Delays and Cancellations, Kaggle. 
https://www.kaggle.com/datasets/usdot/flight-delays?datasetId=810

- Data Expo 2009: Airline on time data, Hardvard Dataverse. 
https://doi.org/10.7910/DVN/HG7NV7