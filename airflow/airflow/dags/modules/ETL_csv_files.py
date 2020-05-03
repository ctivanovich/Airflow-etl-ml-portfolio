def ETL_csv_files(table_name, bucket_name):
    from google.cloud import storage
    import csv, os
    client = storage.Client('...')
    bucket = client.get_bucket(bucket_name)
    for i, blob in enumerate([x for x in bucket.list_blobs(prefix = f"{table_name}/") if '.csv' in x.name]):
        blob.download_to_filename(f"/tmp/{bucket_name}_{table_name}-{i}.csv", client = client)
        try:
            with open(f"/tmp/{bucket_name}_{table_name}-{i}.csv", "r") as infile, open(f"/tmp/temp-{bucket_name}_{table_name}-{i}.csv", "w") as outfile:
                reader = csv.reader(infile)
                writer = csv.writer(outfile, quoting=csv.QUOTE_ALL, lineterminator='\n')
                for row in reader:
                    writer.writerow([r if r != '' else r'\N' for r in row])
        finally:
            os.remove(f"/tmp/{bucket_name}_{table_name}-{i}.csv")