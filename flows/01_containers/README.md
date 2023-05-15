URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"


# Cada vez que cambies el codigo ingest_data.py tienes que hacer un build
# Construye imagen de python container
docker build -t taxi_ingest .

docker run -it --network=pg-network taxi_ingest --table_name=yellow_taxi_trips --url=https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz 

# En caso de no aparecer GCSBucket, instalar prefect_gcs