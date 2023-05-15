# bigdataTutorial

## 1.- Instala Docker Desktop en tu computadora 

https://www.docker.com/products/docker-desktop/

## 2.- Instala Visual Studio Code en tu computadora 

https://code.visualstudio.com/download

## 3.- Instala Anaconda en tu computadora 

https://www.anaconda.com/download

## 4.- Sigue el siguiente tutorial para instalar wget

https://builtvisible.com/download-your-website-with-wget/


# Instrucciones a seguir en el taller

## Ejercicio #1

- Descargar este repositorio
- Abir Docker
- Abrir Visual Studio Code
- Entrar a ./flows/01_contenedores
- Hacer build a contenedor de script ingest_data.py

```
docker build -t taxi_ingest .
```

- Levantar servicios de base de datos con docker-compose

```
docker compose up
```

- Crear servidor pg-database
- Correr primer script

```
docker run -it --network=pg-network taxi_ingest --table_name=yellow_taxi_trips --url=https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz 
```

## Ejercicio #2

- Regresa a el directorio ./bigdataTutorial
- Crear conda enviroment

```
conda create -n sii_bigdata python=3.9
```

- Activar conda enviroment e instalar pip

```
conda activate sii_bigdata
```
```
conda install pip
```
- Instalar requirements.txt
```
pip install -r /path/to/requirements.txt
```
- Ve al directorio flows/02_etl
- Inicia Prefect Orion
```
prefect orion start
```
- Crea tu bloque de GCS Bucket
- Corre script en local
```
python etl_web_to_gcs.py
```
