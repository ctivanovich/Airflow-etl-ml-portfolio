FROM "google/cloud-sdk:slim"

ENV GCLOUD_PROJECT_ID=....
ENV GOOGLE_COMPUTE_ZONE=....

# Necessary because cloud-sdk image lagging behind its own base debian image
RUN apt-get update --fix-missing

# Authenticate GCP with service account key
COPY service_account.json service_account.json
RUN gcloud auth activate-service-account --key-file=service_account.json \
&& gcloud --quiet config set project ${GCLOUD_PROJECT_ID} \
&& rm service_account.json

RUN apt-get install python3-minimal python3-pip -y

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt