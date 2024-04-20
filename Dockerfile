
FROM python:3.8 as cvxpy-installer
RUN apt-get update && apt-get install -y \
    libopenblas-dev \
    liblapack-dev \
    libatlas-base-dev \
    gfortran
COPY requirements.txt .
RUN pip install -r requirements.txt


FROM apache/airflow:2.8.4
COPY --from=cvxpy-installer /usr/local/lib/python3.8/site-packages /home/airflow/.local/lib/python3.8/site-packages
