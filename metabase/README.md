## Current Metabase set-up and configuration
1. Running and accepting https requests at https://mb.lab.linkbal.com.
2. Connections are SSL-certified thanks to free certificates provided by Let's Encrypt and obtained/signed using cert-bot.
3. Automatically starts up at VM boot time, and runs within a Debian 10 (Buster) OS as a registered systemctl service.
4. Data for the service persist in a back-end database named `metabase` and hosted in our `cloud-sql` Google Cloud SQL instance.
5. Essential variable definitions are provided to the service process at boot time in `/usr/bin/metabase_start.sh`.

## New instance of Metabase set-up and requirements

This is being run in a Google Compute Engine Virtual Machine as a service, using the metabase.jar program made available at metabase.com. 


#### Basic Deployment
1. Prepare a backend db in Cloud SQL or elsewhere (by default, MB uses a locally .h5 database).
Example config to set the Metabase backend db:
  - MB_DB_HOST: xx.xx.xx.xx (IP address of sql instance)
  - MB_DB_DBNAME: metabase
  - MB_DB_USER: metabase
  - MB_DB_PASS: metabase
  - MB_DB_PORT: 3306 or 5432 (mysql or postgre)
2. Ensure that the SQL instance can accept connections from the static IP address of the VM.
3. Run `java -jar metabase.jar`, after exporting the appropriate variables.
4. The "MB Jetty Server (UI)" can be accessed on port 3000 (if the VM is configured to accept connections via the IP being connected from -- i.e. linkbal office or an AWS workspace).

### Adding Google OAuth login
In the Metabase admin panel, enable this as an option, the instructions are provided right there in the panel, but the process is more or less as follows:
1. Assign the VM a domain name (optional, but preferable). This requires registers the VM's static IP address with a domain name provider--but regardless, request this of our systems administrator.
2. Register the address as a redirect URL with Google OAuth Credentials (panel > APIs and Services > Credentials), get a client ID and a client secret. Be careful to get the exact address of metabase correctly, e.g. http://34.55.88.191:3000, since it by default will listen on port 3000 (which also needs to be open to the IP address)--you probably don't have permission to do this, ask our administrator.
3. Provide the OAuth Client ID to Metabase via the admin panel.
4. Connect to Metabase and sign in with your Google @linkbal.co.jp account. If you made a mistake in the redirect URL, you will see an error. 

#### Adding additional data sources

1. Ensure that the IP address of the Metabase VM is whitelisted in the database for any additional databases we want to connect to (e.g. cloud-sql Network settings). 
  - This is necessary, because Metabase is accessing the sql instances directly.
2. Add the new database config via the metabase admin panel in the UI. 

If you are unable to add the data source to Metabase, ensure the network and config settings are correct.

### Advanced: Configuring HTTPS secure connections using a CA-signed SSL certificate
This was somewhat advanced, but the following tutorials allowed me to set this up:

Configure MB: https://www.metabase.com/docs/latest/operations-guide/customizing-jetty-webserver.html
Get a free SSL certificate: https://certbot.eff.org/lets-encrypt/debianbuster-nginx
Use it to create a Java keystore: https://www.digitalocean.com/community/tutorials/java-keytool-essentials-working-with-java-keystores
