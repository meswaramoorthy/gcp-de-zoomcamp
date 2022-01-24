docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v d:/CodingWorkspace/data-engineering-zoomcamp/gcp-data-engineering/1_docker_sql/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  postgres:13

docker run -it \
  -e 'PGADMIN_DEFAULT_EMAIL=admin@admin.com' \
  -e 'PGADMIN_DEFAULT_PASSWORD=root' \
  -p 8080:80 \
  dpage/pgadmin4
