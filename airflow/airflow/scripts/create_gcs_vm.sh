!/bin/bash

### variables
PROJECT=$1
TABLE=$2

### gcloud disk create
echo ${PROJECT}.${TABLE}

/snap/bin/gcloud compute --project "..." disks create "...-${PROJECT}-${TABLE}" \
               --size ".." \
               --zone "..." \
               --source-snapshot "..." \
               --type ".."

sleep 60
