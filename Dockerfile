FROM quay.io/astronomer/astro-runtime:12.8.0

# RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
#     pip install --no-cache-dir dbt-bigquery && deactivate

ENV VENV_PATH="/usr/local/airflow/dbt_venv"
ENV PATH="$VENV_PATH/bin:$PATH"

RUN python -m venv $VENV_PATH && \
    source $VENV_PATH/bin/activate && \
    pip install --upgrade pip setuptools && \
    pip install --no-cache-dir dbt-bigquery==1.5.3 pandas Faker pyarrow numpy && \
    deactivate

RUN echo "source $VENV_PATH/bin/activate" > /usr/local/airflow/dbt_env.sh
RUN chmod +x /usr/local/airflow/dbt_env.sh

COPY ./gcp/service_account.json /usr/local/airflow/dbt/service_account.json