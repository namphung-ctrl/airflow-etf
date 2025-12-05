mkdir -p plugins logs dags config
docker build . --tag pyrequire_airflow:2.7.1
docker compose down
docker compose up -d
