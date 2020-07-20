def ETL_csv_files(gcs_project_id, bucket_name, table_name):
    from google.cloud import storage
    import csv, os, glob
    client = storage.Client(gcs_project_id)
    bucket = client.get_bucket(bucket_name)

    # cleanup local temporary files
    tempfile_path_list = glob.glob(f"/tmp/{bucket_name}_{table_name}-*.csv") + \
                         glob.glob(f"/tmp/temp-{bucket_name}_{table_name}-*.csv")
    for tempfile_path in tempfile_path_list:
        os.remove(tempfile_path)

    for i, blob in enumerate([x for x in bucket.list_blobs(prefix = f"{table_name}/") if '.csv' in x.name]):
        blob.download_to_filename(f"/tmp/{bucket_name}_{table_name}-{i}.csv", client = client)
        try:
            with open(f"/tmp/{bucket_name}_{table_name}-{i}.csv", 'r') as infile, open(f"/tmp/temp-{bucket_name}_{table_name}-{i}.csv", 'w') as outfile:
                reader = csv.reader(infile)
                writer = csv.writer(outfile, quoting=csv.QUOTE_ALL, lineterminator='\n')
                for row in reader:
                    writer.writerow([r if r != '' else r'\N' for r in row])
        finally:
            os.remove(f"/tmp/{bucket_name}_{table_name}-{i}.csv")
