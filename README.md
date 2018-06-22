# Apache Ranger S3 Plugin

Ranger S3 Plugin enabled creation of policies in Apache Rangere for S3 buckets hosted on Ceph/RadosGW (S3 coming later). 
It merely allows for the creation of policies and does not set acls by itself. It can be used together with its sister 
`golang` project [s3gw](http://github.com/bolkedebruin/s3gw).

# Installation

1. Build the jar as per usual. If you do not have a local Apache Ranger installation to test against use 
`mvn package -DskipTests`.
2. Copy the jar to `${RANGER_HOME}/ews/webapp/WEB-INF/classes/ranger-plugins/s3`. Please note that the location
is important (`s3`). 
3. Load the service definition into Apache Ranger. 
`curl -u <admin>:<admin> -d "@s3-ranger.json" -X POST -H "Accept: application/json" -H "Content-Type: application/json" 
http://{RANGER_HOST}:{RANGER_PORT}/service/public/v2/api/servicedef`
4. Configure the service in Apache Ranger by logging in to the Web UI.

# Roadmap

* Proper lookups
* No ceph-user name required
* AWS S3 support
