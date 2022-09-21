deploy_dev:
	touch ./gitignore_dummy_file
	gsutil cp gitignore_dummy_file gs://us-central1-composer-develo-c7f84f4f-bucket/dags/examples/data_ingestion/ingestao_mysql_signup_serverless
	gsutil -m rm -r gs://us-central1-composer-develo-c7f84f4f-bucket/dags/examples/data_ingestion/ingestao_mysql_signup_serverless
	gsutil -m cp -r . gs://us-central1-composer-develo-c7f84f4f-bucket/dags/examples/data_ingestion/ingestao_mysql_signup_serverless
deploy_prod:
	touch ./gitignore_dummy_file
	gsutil cp gitignore_dummy_file gs://us-central1-composer-produc-6c7a2ad8-bucket/dags/ingestao_mysql_signup_serverless
	gsutil -m rm -r gs://us-central1-composer-produc-6c7a2ad8-bucket/dags/ingestao_mysql_signup_serverless
	gsutil -m cp -r . gs://us-central1-composer-produc-6c7a2ad8-bucket/dags/ingestao_mysql_signup_serverless