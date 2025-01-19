FROM python:3.12.8

RUN pip install pandas sqlalchemy psycopg2-binary

WORKDIR /app
COPY homework_week1.py homework_week1.py
COPY green_tripdata_2019-10.csv /app/
COPY taxi_zone_lookup.csv /app/

ENTRYPOINT [ "python", "homework_week1.py" ]