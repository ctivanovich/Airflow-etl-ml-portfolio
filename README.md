## Airflow-etl-ml-portfolio

This a portfolio container Airflow docker-compose and configuration files, Airflow machine learning and data ETL DAG definitions, supporing modules for these operations, machine and deep learning run-time Dockerfile definitions (almost everything here gets containerized at run-time as well), and the structure of the repository is essential to the smooth functioning and updating of Airflow in production.

#### The meaning of ....
I use .... to denote redacted information (such as sensitive dir, variable, and file location information).

### Structure of this repository and explanations
<pre>
.
├── airflow
│   ├── dags
│   │   ├── choose_matched_users.py
│   │   ├── collect_images.py
│   │   ├── collect_recommendations_data.py
│   │   ├── generate_image_recs.py
│   │   ├── generate_implicit_recs.py
│   │   ├── generate_lightfm_recs.py
│   │   ├── split_up_user_recs_for_honjitsu.py
│   │   └── update_datamart_from_biq_query.py
│   └── modules
│       ├── ETL_csv_files.py
│       ├── choose_matched_users_utils.py
│       ├── slack_alert.py
│       └── subset_and_deliver_recs.py
├── docker
│   ├── docker-airflow
│   │   ├── docker-airflow
│   │   │   ├── Dockerfile
│   │   │   ├── config
│   │   │   │   └── airflow.cfg
│   │   │   ├── dags
│   │   │   │   └── tuto.py
│   │   │   └── script
│   │   │       └── entrypoint.sh
│   │   ├── docker-compose-celery.yaml
│   │   ├── docker-compose-local.yaml
│   │   └── docker-compose.yaml
│   └── docker-nbrun
│       ├── base.Dockerfile
│       ├── recommend-cf-v1.Dockerfile
│       ├── recommend-image-v1.Dockerfile
│       ├── recommend-implicit-v1.Dockerfile
│       └── requirements.txt
├── metabase
│   ├── README.md
│   ├── metabase.service
│   └── metabase_start.sh
├── production-ml-nbs
│   └── cl-recommend-users
│       ├── recommend-images-v1
│       │   ├── images-recommend-v1.ipynb
│       │   ├── obtain-images-prod-v1.ipynb
│       │   └── scripts
│       │       ├── __init__.py
│       │       └── hd5_dataset.py
│       ├── recommend-implicit-v2
│       │   └── implicit-svd-v2.ipynb
│       └── recommend-lightfm-v1
│           ├── hyperparameters.yaml
│           ├── lightfm-v1-hyperparameter-search.ipynb
│           ├── lightfm-v1.ipynb
└── service-sql
</pre>
