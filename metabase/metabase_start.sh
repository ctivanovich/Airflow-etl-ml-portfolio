#! /bin/bash

### /usr/bin/metabase_start.sh

DATE=`date '+%Y-%m-%d %H:%M:%S'`
echo "Metabase service started at ${DATE}" | systemd-cat -p info

export JAVA_OPTS="-XX:+IgnoreUnrecognizedVMOptions -Dfile.encoding=UTF-8 --add-opens=java.base/java.net=ALL-UNNAMED --add-modules=java.xml.bind -Djava.awt.headless=true -Djavax.accessibility.assistive_technologies=java.lang.Object" 
export JAVA_TOOL_OPTIONS="-Xmx12g"

export MB_JETTY_SSL="true"
export MB_JETTY_SSL_PORT="443"
export MB_JETTY_SSL_KEYSTORE="/etc/letsencrypt/live/..../keystore.jks"
export MB_JETTY_SSL_KEYSTORE_PASSWORD="I'm a secret!"

export MB_DB_TYPE=mysql
export MB_DB_DBNAME=metabase
export MB_DB_PORT=3306
export MB_DB_USER=metabase
export MB_DB_PASS="I'm a secret!"
export MB_DB_HOST=104.198.112.55

echo "Starting..."
java -jar /opt/metabase/metabase.jar