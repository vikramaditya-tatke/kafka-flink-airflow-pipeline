FROM astrocrpublic.azurecr.io/runtime:3.3-10-python-3.12
USER root
RUN mkdir -p /usr/local/airflow/data && chown astro:0 /usr/local/airflow/data && chmod 0775 /usr/local/airflow/data
USER astro

# Shared libraries are mounted under include by Astro.
ENV PYTHONPATH="/usr/local/airflow:/usr/local/airflow/include"
