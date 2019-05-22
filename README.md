# mvrs-dspa

* prerequisites:
  * docker (make sure dockerd runs)
  * docker compose (linux: not sure what minimum required privileges are; root works, obviously)
  * maven
  * IntelliJ IDEA with Scala plugin
  
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
* Prometheus docker: http://localhost:9091/graph
* Grafana docker: http://localhost:3001/?orgId=1 (no dashboards delivered as part of solution)
* ElasticSearch (to check if online):  

## Troubleshooting
* `Exception in thread "main" java.util.concurrent.ExecutionException: org.apache.kafka.common.errors.TimeoutException: Timed out waiting for a node assignment.`
  * Probable cause: Kafka not running. Start with `docker-compose up` in directory `docker`
