# Airflow-etl-ml-portfolio
This a portfolio containing Airflow, ETL DAGs, ML notebook tasks, Dockerfiles, SQL schema files, and more.

The layout is as follows:

<pre>
├── data-platform
│   ├── airflow
│   │   ├── dags --production DAG definitions
│   │   │   └── modules --supporting and extensible code
│   │   ├── logs
│   │   │   └── scheduler
│   │   └── scripts --VM/server-side scripts
│   └── docker
│       ├── docker-airflow
│       │   └── docker-airflow
│       │       ├── config --Airflow config
│       │       ├── dags --sample DAG definitions
│       │       └── script --entrypoint.sh
│       └── docker-nbrun --definitions for machine and deep learning containers for machine and deep learning
└── nb_production
    └── recommend-users
        ├── recommend-images-v1 --image-based recommendation engine
        │   └── scripts --supporting Class definitions
        └── recommend-implicit-v2 --implicit, SVD+ collaborative filtering approach to recommendations
</pre>
