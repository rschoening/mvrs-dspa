# mvrs-dspa

* prerequisites:
  * docker (make sure dockerd runs)
  * docker compose (linux: not sure what minimum required privileges are; root works, obviously)
  * maven
  * IntelliJ IDEA with Scala plugin
  ![scala plugin](https://github.com/rschoening/mvrs-dspa/blob/master/doc/images/intellij-scala-plugin.png "Scala plugin")

  
* git clone https://github.com/rschoening/mvrs-dspa.git
* cd to mvrs-dsp
* mvn package
* start IDEA -> "Import Project from Maven" (pom.xml in mvrs-dspa)
  * enable "add dependencies for IDEA"
  * select first module only in list (unselect second)
  
## TODO
* run configurations (in git repo) apparently get deleted on mvn project import
* screenshots for import options
* docker not yet working in VM, can't test
* where to put data/set env variable

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
* Grafana docker: http://localhost:3001/?orgId=1 (no dashboards delivered as part of solution)
* ElasticSearch docker (to check if online): http://localhost:9201/

## Troubleshooting
* When starting one of the Flink jobs: `Exception in thread "main" java.util.concurrent.ExecutionException: org.apache.kafka.common.errors.TimeoutException: Timed out waiting for a node assignment.`
  * Probable cause: Kafka not running.
  * Solution: start with `docker-compose up` in directory `docker`
* When running `docker-compose up`: `ERROR: for docker_exporter_1  Cannot start service exporter: driver failed programming external connectivity on endpoint docker_exporter_1 (c1b1acc38d051b138707fbae6e323641332e3f659145eef51ea5764b2a3953e7): Error starting userland proxy: mkdir /port/tcp:0.0.0.0:9101:tcp:172.18.0.2:9100: input/output error`
  * Sometimes observed on Windows on first `docker-compose up`in session.
  * Solution: restart docker, try again

