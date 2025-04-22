# dbt-bigquery-project

This project is intended to get an understanding of the powerful combination of dbt and BigQuery!

 ## Run the project locally

 ### Setup astro
 - Install astro via the following command: `curl -sSL install.astronomer.io | sudo bash -s`
 - Move to the astro folder
 - Initialize the project: `astro dev init`
 - Start Airflow by running: `astro dev start`
 
 ### Create a virtual environment (Make sure that pip is installed )
 
 - python3 -m venv name_of_env
 - source name_of_env/bin/activate
 - pip3 install -r requirements.txt
 
 ### Using the starter project
 
 Try running the following commands:
 - dbt init
 - dbt debug
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
