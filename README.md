# dbt-bigquery-project

This project is intended to get an understanding of the powerful combination of dbt and BigQuery!

## Run the project locally
 
### Create a virtual environment (Make sure that pip is installed )
 - python3 -m venv name_of_env
 - source name_of_env/bin/activate
 - pip3 install -r requirements.txt

### Setup astro
 - Install astro via the following command inside the virtual environment: `curl -sSL install.astronomer.io | sudo bash -s`
 - Initialize the project: `astro dev init`
 - Start Airflow by running: `astro dev start`
 - Access Airflow UI at http://localhost:8080

In case the Dockerfile is modified, just run `astro dev restart` to restart the container.

### Configure Airflow Connection to GCP:

- Place service_account.json in `/usr/local/airflow/gcp/`
- Edit .env: `AIRFLOW__CORE__TEST_CONNECTION=Enabled`

### Set Up Google Cloud Credentials

In Airflow UI (Admin > Connections), add:
- Connection ID: `the id of your connexion`
- Connection Type: `Google Bigquery`
- Project ID: `your_project_id`
- Keyfile Path: `/usr/local/airflow/gcp/service_account.json`
  
Test the connection.

### Using the starter project
 
 Try running the following commands after moving to the `dbt` folder:
 - dbt init
 - dbt debug
 - dbt test
 - dbt run
   
 If you wish to run a specific model, use the following command: `dbt run --model name_of_model`
 
 If you want to generate automatically the documentation:
 - dbt docs generate
 - dbt docs serve
  
### Resources:
 - Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
 - Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
 - Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
 - Find [dbt events](https://events.getdbt.com) near you
 - Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
