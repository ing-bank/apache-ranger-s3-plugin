#!/bin/bash

docker-compose exec -T ceph s3cmd put /etc/issue s3://demobucket/subdir1/
docker-compose exec -T ceph s3cmd put /etc/issue s3://demobucket/subdir2/

