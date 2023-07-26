FROM postgres
COPY init.sql /docker-entrypoint-initdb.d/
COPY data.csv /docker-entrypoint-initdb.d/
