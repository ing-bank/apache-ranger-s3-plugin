[![Build Status](https://travis-ci.org/ing-bank/apache-ranger-s3-plugin.svg?branch=master)](https://travis-ci.org/ing-bank/apache-ranger-s3-plugin)
[![codecov.io](http://codecov.io/github/ing-bank/apache-ranger-s3-plugin/coverage.svg?branch=master)](https://codecov.io/gh/ing-bank/apache-ranger-s3-plugin?branch=master)

# Apache Ranger S3 Plugin

Ranger S3 Plugin enables creation of policies in Apache Ranger for S3 buckets hosted on Ceph/RadosGW (S3 coming later). 
It merely allows for the creation of policies and does not set ACLs by itself. It can be used together with its sister 
`scala` project [Airlock](https://github.com/ing-bank/airlock).

# Installation

1. Run ceph demo container using docker-compose
```
docker-compose up
```
It will start Ceph demo image on port 8010. 

2. Build the plugin jar using maven 
```
mvn package
``` 
If you do not have a local Ceph installation to test against use `mvn package -DskipTests`.

3. Copy the jar to `${RANGER_HOME}/ews/webapp/WEB-INF/classes/ranger-plugins/s3`. Please note that the location
is important (`s3`). 

4. Load the service definition into Apache Ranger. 
```
curl -u <admin>:<admin> -d "@s3-ranger.json" -X POST -H "Accept: application/json" -H "Content-Type: application/json" 
http://{RANGER_HOST}:{RANGER_PORT}/service/public/v2/api/servicedef
```
5. Configure the service in Apache Ranger by logging in to the Web UI.

# Roadmap

* Proper lookups
* No ceph-user name required
* AWS S3 support

# Ranger admin site configuration

Ranger S3 plugin uses aws sdk to connect to backend to list buckets and folders. In case of 
timeouts, check ranger setting and adjust accordingly. Default 1000 may be to low in some cases.
Configuration file is in `ranger-1.1.0-admin/ews/webapp/WEB-INF/classes/conf/ranger-admin-default-site.xml.`

```
<property>
    <name>ranger.resource.lookup.timeout.value.in.ms</name>
    <value>10000</value>
    <description />
</property>
```