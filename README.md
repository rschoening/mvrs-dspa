# DSPA 2019 semester project

## Prerequisites
* docker
* docker compose
* maven
* Java 1.8
* IntelliJ IDEA with Scala plugin:
<img src="https://github.com/rschoening/mvrs-dspa/blob/master/doc/images/intellij-scala-plugin.png" alt="Scala plugin" width="60%"/>

## Setting up the development environment
1. cd to parent directory for project, enter `git clone https://github.com/rschoening/mvrs-dspa.git`
1. copy the csv test data directories `streams`and `tables` from `1k-users-sorted` or `10k-users-sorted` to the subdirectory `docker/data` of the repository
1. set environment variable `MVRS_DSPA_DATA_DIR` to the absolute file URI to the repository subdirectory `docker/data` such that it is seen by IDEA (e.g. `export MVRS_DSPA_DATA_DIR=file:///dspa19/projects/mvrs-dspa/docker/data` in `.profile`)
1. start IntelliJ IDEA -> "Import Project" (selecting `pom.xml` in `mvrs-dspa`), accepting all defaults. Unfortunately, during the import process the configured run configurations are deleted. To bring them back:
   1. close IDEA again
   1. cd to the project directory
   1. enter `git checkout -- .idea/runConfigurations` 
   1. start IDEA again and open the project
   1. confirm that the run configurations (drop down list in upper right of IDEA window) are available (order can differ):
   <img src="https://github.com/rschoening/mvrs-dspa/blob/master/doc/images/idea-run-configurations.png" alt="IDEA run configurations" width="60%"/>
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
   2. go to `Management`-> `Saved Objects`
   3. import `export.json` from `mvrs-dspa/docker/kibana`
    <img src="https://github.com/rschoening/mvrs-dspa/blob/master/doc/images/kibana-saved-objects-import.png" alt="Kibana import objects" width="60%"/>
   4. go to `Index patterns` and *star* one of the listed index patterns. Any will do (otherwise the imported dashboards are not listed)
   <img src="https://github.com/rschoening/mvrs-dspa/blob/master/doc/images/kibana-index-patterns-star.png" alt="Kibana import objects" width="60%"/>

## Running the Flink jobs
### Overview
* The data preparation and analytic jobs can be configured using the [application.conf](https://github.com/rschoening/mvrs-dspa/blob/master/src/main/resources/application.conf) file. The individual settings are explained in that file.
* The analytic jobs depend on the data to have previously been loaded using the data preparation jobs. There are no dependencies between analytic jobs for different project tasks.
* Note that restarting the Kafka container resets the topics. Also, when staring jobs that write to a Kafka topic, the topic is first deleted and recreated if it exists. Consequences are:
  * after starting the docker container for Kafka, the data preparation job that writes events to Kafka must be re-run (the same is not true for the ElasticSearch indices, which are maintained across container restarts in a docker volume)
  * when starting a job that writes to a Kafka topic, there should not be another job running that reads from the same topic.

### Data preparation
The following two jobs must have been run prior to running any of the analytics jobs. As noted above, the second of the jobs (writing to Kafka) needs to be re-run after restarting the Kafka container. The first job (writing static data to ElasticSearch) must be re-run after changing the LSH configuration relevant for the recommendations task, as the MinHash/LSH configuration in the prepared static data and the recommendation job must match for meaningful results.
The two jobs terminate in less than a minute total, for the low-volume testdata.

#### Loading static data into ElasticSearch
* Inputs:
  * csv files in testdata directory `tables` (location configured in `application.conf` and environment variable `MVRS_DSPA_DATA_DIR`)
* Outputs:
  * ElasticSearch indexes `mvrs-recommendation-person-features`, `mvrs-recommendation-forum-features`, `mvrs-recommendation-person-minhash`, `mvrs-recommendation-known-persons`, `mvrs-recommendation-lsh-buckets`
* Job class: `org.mvrs.dspa.jobs.preparation.LoadStaticDataJob`
* in IDEA, execute the run configuration `Preparation: load static tables (csv -> ElasticSearch)`
   * The run configuration sets the program argument `local-with-ui` to launch the Flink dashboard UI. This can be removed if multiple jobs should be run simultaneously.
   * The job can be re-run as needed. Output indices are deleted and recreated if they already exist.
* Checking results:
   * the load progress and final result can be observed in Kibana:
      * Index management:
      <img src="https://github.com/rschoening/mvrs-dspa/blob/master/doc/images/kibana-index-management.png" alt="Kibana index management" width="60%"/>
      * Discover (make sure to set time period to something like 'Last 15 minutes', as the static data is timestamped with insertion time):
      <img src="https://github.com/rschoening/mvrs-dspa/blob/master/doc/images/kibana-discover-static-data-loading.png" alt="Kibana index management" width="60%"/>

#### Writing events to Kafka
* Job class: `org.mvrs.dspa.jobs.preparation.WriteEventsToKafkaJob`
* in IDEA, execute the run configuration `Preparation: load events (csv -> Kafka)`
   * The run configuration sets the program argument `local-with-ui` to launch the Flink dashboard UI. This can be removed if multiple jobs should be run simultaneously.

##### NOTES
  * To simulate out-of-order events, the value for `data.random-delay` in `application.conf` file can be modified prior to loading the events into Kafka. The random delay is used to parameterize a normal distribution of random delays, with a mean of 1/4 and standard deviation of 1/2 of the configured value, capping the distribution at that value (see `org.mvrs.dspa.utils.FlinkUtils.getNormalDelayMillis()`)  
  * The speedup factor (`data.speedup-factor` in `application.conf`) is only applied during the analytic jobs, not during data preparation.
  * To allow precise control over the ordering and lateness of events when reading from Kafka, the preparation job uses a single worker, and writes to a single Kafka partition. Doing otherwise would create additional sources of un-ordering that would not allow exact control of lateness/reordering based on defined values for `data.random-delay` and `data.max-out-of-orderness`.

### Active post statistics
#### Calculating post statistics
* Inputs
   * Kafka topics:
* Outputs
   * Kafka topic
   * ElasticSearch index 
* Job class: `org.mvrs.dspa.jobs.activeposts.ActivePostStatisticsJob`
* in IDEA, execute the run configuration `Task 1.1: active post statistics (Kafka -> Kafka, post info: Kafka -> ElasticSearch)`
   * The run configuration sets the program argument `local-with-ui` to launch the Flink dashboard UI. This can be removed if multiple jobs should be run simultaneously.
* Checking results:
  * run second job to write to ElasticSearch index
#### Notes
* speedup
* max-out-of-orderness
* job-specific parameters

#### Writing post statistics results to ElasticSearch index
* Inputs
  * Kafka topic
  * ElasticSearch index
* Outputs
  * ElasticSearch index
* Job class: `org.mvrs.dspa.jobs.activeposts.WriteActivePostStatisticsToElasticSearchJob`
* In IDEA, execute the run configuration `Task 1.2: active post statistics - (Kafka -> ElasticSearch) [NO UI]`
  * The run configuration does _not_ set the argument `local-with-ui`, to allow for parallel execution with previous task on local machine/minicluster.
* Checking results:
   * Kibana dashboard: ....
* TODO execution plan image
#### Notes
* speedup
* max-out-of-orderness
* job-specific parameters

### Recommendations
* Inputs (created by data preparation jobs, which have to be run before, see above): 
   * Event topics in Kafka: `mvrs_comments`, `mvrs_likes`, `mvrs_posts`
   * ElasticSearch indexes with static data: `mvrs-recommendation-person-features`, `mvrs-recommendation-forum-features`, `mvrs-recommendation-person-minhash`, `mvrs-recommendation-known-persons`, `mvrs-recommendation-lsh-buckets`
* Outputs (re-generated automatically when the job starts):
   * ElasticSearch index with recommendation documents: `mvrs-recommendations`
   * ElasticSearch index with post features: `mvrs-recommendation-post-features`
* In IDEA, execute the run configuration `Task 2: user recommendations (Kafka -> ElasticSearch)` 
   * Job class: `org.mvrs.dspa.jobs.recommendations.RecommendationsJob`
   * The run configuration sets the program argument `local-with-ui` to launch the Flink dashboard UI. This can be removed if multiple jobs should be run simultaneously.
* Checking results:
   * Kibana dashboard: [\[DSPA\] Recommendations](http://localhost:5602/app/kibana#/dashboard/7c230710-6855-11e9-9ba6-39d0e49adb7a)
* TODO execution plan image
#### Notes
* speedup
* max-out-of-orderness
* job-specific parameters

### Unusual activity detection
* Job class: `org.mvrs.dspa.jobs.clustering.UnusualActivityDetectionJob [local-with-ui]`
* IDEA run configuration: `Task 3: unusual activity detection (Kafka -> ElasticSearch)`  (specified `local-with-ui` to launch the Flink dashboard UI)
* Kibana dashboard: 
   * Unusual activity detection: cluster metadata graph can have gaps since that visualization does not interpolate across buckets with nodata (which may result due to extending windows)
* TODO execution plan image
#### Notes
* speedup
* max-out-of-orderness
* job-specific parameters

## Solution overview
### Package structure
```
src
└─ main
│  └─ resources
│  │  └─ application.conf            | configuration file (default values; file with overrides can be placed on classpath)
│  └─ scala
│     └─ org.mvrs.dspa               | root package of Scala solution
│        └─ db                       | package with data access types for ElasticSearch 
|        │    ElasticSearch.scala    | - static registry of ElasticSearch indices
│        └─ functions                | package for stream functions that are not strictly tied to one job
│        └─ jobs                     | package with job implementations 
|        │  └─ activeposts           | package for active post statistics jobs
|        │  └─ clustering            | package for unusual activity detection job
|        │  └─ preparation           | package for data preparation jobs (static tables, events)
|        │  └─ recommendations       | package for user recommendations job
│        └─ model                    | package for domain model types
│        └─ streams                  | package for input streams (csv, Kafka)
|        │    package.scala          | methods for reading input streams (csv or Kafka, comments raw or resolved)
|        │    KafkaTopics.scala      | static registry of Kafka topics
│        └─ utils                    | (more or less) generic utilities 
|        Settings.scala              | Object for accessing settings in application.conf
└─ test                              | tests and test resources
│  └─ resources                      |
│  │  └─ resources                   |
│  │     └─ streams                  | directory with reduced streaming test data files (csv)
│  └─ scala
│     └─ categories                  | package for definition of test categories
│     └─ db
│     └─ functions
│     └─ jobs
│     └─ streams
│     └─ utils
└─ target
   └─ site
   │  └─ scaladoc
   │        index.html
   │  mvrs-dspa-1.0.jar
```
### Configuration
* application.conf (based on https://github.com/lightbend/config/blob/master/README.md)
### Unit tests
### Scaladoc
can be generated with mvn scala:doc
### Unit tests
* Unit tests: Scalatest
* Integration tests: JUnit
* run configurations
### ElasticSearch
* ElasticSearch indexes: see class xy
### Kafka
* Kafka topics: see class xy
### Kibana
* Dashboards

## Addresses:
* Flink lokal UI: http://localhost:8081
* Flink docker: http://localhost:8082
* Kibana docker: http://localhost:5602
  * Active post statistics: http://localhost:5602/app/kibana#/dashboard/a0af2f50-4f0f-11e9-acde-e1c8a6292b89
  * Recommendations: http://localhost:5602/app/kibana#/dashboard/7c230710-6855-11e9-9ba6-39d0e49adb7a
  * Activity detection: http://localhost:5602/app/kibana#/dashboard/83a893d0-6989-11e9-ba9d-bb8bdc29536e
* Prometheus docker: http://localhost:9091
* Grafana docker: http://localhost:3001 (no dashboards delivered as part of solution; initial login with admin/admin, then change pwd)
* ElasticSearch docker (to check if online): http://localhost:9201

## Troubleshooting
* Some symptoms and solutions are listed [here](https://github.com/rschoening/mvrs-dspa/blob/master/doc/Troubleshooting.md)
