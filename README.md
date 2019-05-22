# DSPA 2019 semester project

## Prerequisites
* docker
* docker compose
* maven
* Java 1.8
* IntelliJ IDEA with Scala plugin:
![scala plugin](https://github.com/rschoening/mvrs-dspa/blob/master/doc/images/intellij-scala-plugin.png "Scala plugin")

## Setting up the development environment
1. cd to parent directory for project, enter `git clone https://github.com/rschoening/mvrs-dspa.git`
1. copy the csv test data directories `streams`and `tables` from `1k-users-sorted` or `10k-users-sorted` to the subdirectory `docker/data` of the repository
1. set environment variable `MVRS_DSPA_DATA_DIR` to the absolute file URI to the repository subdirectory `docker/data` such that it is seen by IDEA (e.g. `export MVRS_DSPA_DATA_DIR=file:///dspa19/projectst/mvrs-dspa/docker/data` in `.profile`)
1. start IntelliJ IDEA -> "Import Project" (selecting `pom.xml` in `mvrs-dspa`), accepting all defaults. Unfortunately, during the import process the configured run configurations are deleted. To bring them back:
   1. close IDEA again
   1. cd to the project directory
   1. enter `git checkout -- .idea/runConfigurations` 
   1. start IDEA again and open the project
   1. confirm that the run configurations (drop down list in upper right of IDEA window) are available:
   ![run configurations](https://github.com/rschoening/mvrs-dspa/blob/master/doc/images/idea-run-configurations.png "IDEA run configurations")
1. In `Terminal` tab in IDEA: run `mvn clean package`

## Setting up the runtime environment
1. make sure that `dockerd` is running
1. cd to `mvrs-dspa\docker`
1. as su, enter `docker-compose up -d`
1. check that all containers were started successfully: enter `docker-compose ps` The following containers should be listed:
```
            Name                       Command               State                         Ports                       
--------------------------------------------------------------------------------------------------------------------
docker_elasticsearch_1   /usr/local/bin/docker-entr ...   Up      0.0.0.0:9201->9200/tcp, 9300/tcp                  
docker_exporter_1        /bin/node_exporter               Up      0.0.0.0:9101->9100/tcp                            
docker_grafana_1         /run.sh                          Up      0.0.0.0:3001->3000/tcp                            
docker_jobmanager_1      /docker-entrypoint.sh jobm ...   Up      6123/tcp, 0.0.0.0:8082->8081/tcp, 9249/tcp        
docker_kafka_1           start-kafka.sh                   Up      0.0.0.0:9092->9092/tcp, 9093/tcp                  
docker_kibana_1          /usr/local/bin/kibana-docker     Up      0.0.0.0:5602->5601/tcp                            
docker_prometheus_1      /bin/prometheus --config.f ...   Up      0.0.0.0:9091->9090/tcp                            
docker_taskmanager_1     /docker-entrypoint.sh task ...   Up      6121/tcp, 6122/tcp, 6123/tcp, 8081/tcp, 9249/tcp  
docker_zookeeper_1       /bin/sh -c /usr/sbin/sshd  ...   Up      0.0.0.0:2181->2181/tcp, 22/tcp, 2888/tcp, 3888/tcp
```
   * in case `docker_elasticsearch_1` is not listed:
      1. as su, enter `sysctl -w vm.max_map_count=262144` (see https://www.elastic.co/guide/en/elasticsearch/reference/6.7/vm-max-map-count.html).
      1. `docker-compose down`
      1. `docker-compose up -d`
      1. check again with `docker-compose ps`
1. Import the dashboards in Kibana:
   1. open Kibana in the browser, at http://localhost:5602/
   2. go to `Management`-> `Saved Objects` *TODO SCREENSHOT*
   3. import `export.json` from `mvrs-dspa/docker/kibana`
   4. go to `Index patterns` and *star* one of the listed index patterns. Any will do (otherwise the imported dashboards are not listed) *TODO SCREENSHOT*

## Running the Flink jobs
* TODO

## Solution layout
* package structure
* configuration (application.conf)
* unit tests

## NOTE
* if bind address error occurs when starting job that starts the web UI, then check if the flink dashboard is still open in a browser window. The client keeps the port open. 
* Unusual activity detection: cluster metadata graph can have gaps since that visualization does not interpolate across buckets with nodata (which may result due to extending windows)
* recommendations dashboard: set date range to "last 15 minutes"
* save good start date ranges for all kibana dashboards

## Addresses:
* Flink lokal UI: http://localhost:8081/#/overview
* Flink docker: http://localhost:8082/#/overview
* Kibana docker: http://localhost:5602/
  * Active post statistics: http://localhost:5602/app/kibana#/dashboard/a0af2f50-4f0f-11e9-acde-e1c8a6292b89
  * Recommendations: http://localhost:5602/app/kibana#/dashboard/7c230710-6855-11e9-9ba6-39d0e49adb7a
  * Activity detection: http://localhost:5602/app/kibana#/dashboard/83a893d0-6989-11e9-ba9d-bb8bdc29536e
* Prometheus docker: http://localhost:9091/graph
* Grafana docker: http://localhost:3001/?orgId=1 (no dashboards delivered as part of solution; initial login with admin/admin, then change pwd)
* ElasticSearch docker (to check if online): http://localhost:9201/

## Troubleshooting
* When starting one of the Flink jobs: `Exception in thread "main" java.util.concurrent.ExecutionException: org.apache.kafka.common.errors.TimeoutException: Timed out waiting for a node assignment.`
  * Probable cause: Kafka not running.
  * Solution: start with `docker-compose up` in directory `docker`
* When running `docker-compose up`:
  * `ERROR: for docker_exporter_1  Cannot start service exporter: driver failed programming external connectivity on endpoint docker_exporter_1 (c1b1acc38d051b138707fbae6e323641332e3f659145eef51ea5764b2a3953e7): Error starting userland proxy: mkdir /port/tcp:0.0.0.0:9101:tcp:172.18.0.2:9100: input/output error`
    * Sometimes observed on Windows on first `docker-compose up`in session.
    * Solution: restart docker, try again
  * on linux: `ERROR: Couldn't connect to Docker daemon at http+docker://localhost - is it running?`
    * start `dockerd` as su
* failure to start container elasticsearch (best diagnosed when starting that container individually, using 
`docker-compose up elasticsearch`: 
```
elasticsearch_1  | [1]: max virtual memory areas vm.max_map_count [65530] is too low, increase to at least [262144]
elasticsearch_1  | [2019-05-22T12:02:31,504][INFO ][o.e.n.Node               ] [kdPY8cQ] stopping ...
elasticsearch_1  | [2019-05-22T12:02:31,569][INFO ][o.e.n.Node               ] [kdPY8cQ] stopped
elasticsearch_1  | [2019-05-22T12:02:31,570][INFO ][o.e.n.Node               ] [kdPY8cQ] closing ...
elasticsearch_1  | [2019-05-22T12:02:31,598][INFO ][o.e.n.Node               ] [kdPY8cQ] closed
elasticsearch_1  | [2019-05-22T12:02:31,608][INFO ][o.e.x.m.p.NativeController] [kdPY8cQ] Native controller process has stopped - no new native processes can be started
INFO[2019-05-22T14:02:31.910676430+02:00] shim reaped                                   id=22aa3aa2198d5def930036fa3f0ca2e07fe27c6519bf98b611bb653b4a69f457
INFO[2019-05-22T14:02:31.921933389+02:00] ignoring event                                module=libcontainerd namespace=moby topic=/tasks/delete type="*events.TaskDelete"
docker_elasticsearch_1 exited with code 78
```
  * as su: `sysctl -w vm.max_map_count=262144`
  * see https://www.elastic.co/guide/en/elasticsearch/reference/6.7/vm-max-map-count.html

* when starting unusual activity task: `Exception in thread "main" org.apache.flink.runtime.client.JobExecutionException: Job execution failed. ... Caused by: java.io.FileNotFoundException: The provided file path file:/c:/data/dspa/project/10k-users-sorted/mvrs/activity-classification.txt does not exist.`
  * copy parameter file `./docker/data/mvrs/activity-classification.txt` to a subdirectory `mvrs` in the data directory indicated by the environment variable `MVRS_DSPA_DATA_DIR`
