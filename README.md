# DSPA 2019 semester project

## Setup overview (details below)
1. `git clone https://github.com/rschoening/mvrs-dspa.git`
1. cd to `mvrs-dspa`, run `mvn package` to produce `target/mvrs-dspa-1.0.jar` (to submit to Flink cluster, job classes per project task are listed below)
1. copy csv test data to `mvrs-dspa/docker/data` (-> subdirectories `streams` and `tables`)
1. set environment variable `MVRS_DSPA_DATA_DIR` to data directory path (e.g. `export MVRS_DSPA_DATA_DIR=file:///dspa19/projects/mvrs-dspa/docker/data` in `.profile`)
1. import project (`pom.xml`) in IDEA using default settings, and restore the deleted run configurations using `git checkout -- .idea/runConfigurations`. Restart IDEA
1. as privileged user:
   1. make sure that `dockerd` is running
   1. in `mvrs-dspa/docker` run `docker-compose up -d` (takes a while to download all images, first time only)
   1. check if all containers are up (see caveat below)
1. import Kibana dashboards (`mvrs-dspa/docker/kibana/export.json`)
1. in IDEA: run data preparation jobs 
   * `Preparation: load static tables (csv -> ElasticSearch)`
   * `Preparation: load events (csv -> Kafka)` (redo this after restarting docker containers)
1. Done. The analytic jobs can now be run in any order and repeatedly from IDEA (or by submitting jar to Flink container). Check results in Kibana dashboards on http://localhost:5602. Most jobs launch the Flink dashboard also when run from IDEA.
   * `Task 1.1: active post statistics (Kafka -> Kafka, post info: Kafka -> ElasticSearch)`
   * `Task 1.2: active post statistics - (Kafka -> ElasticSearch) [NO UI]` (start after 1.1)
   * `Task 2: user recommendations (Kafka -> ElasticSearch)`
   * `Task 3: unusual activity detection (Kafka -> ElasticSearch)`

## Prerequisites
* docker
* docker compose
* Maven 3.6
* Java 1.8
* IntelliJ IDEA 2019.1
* Scala plugin for IntelliJ IDEA:
<img src="https://github.com/rschoening/mvrs-dspa/blob/master/doc/images/intellij-scala-plugin.png" alt="Scala plugin" width="60%"/>

## Setting up the development environment
1. cd to parent directory for project, enter `git clone https://github.com/rschoening/mvrs-dspa.git`
1. copy the csv test data directories `streams` and `tables` from `1k-users-sorted` or `10k-users-sorted` to the subdirectory `docker/data` of the repository (it is recommended to start with `1k-users-sorted` for the initial setup)
1. set environment variable `MVRS_DSPA_DATA_DIR` to the absolute file URI to the repository subdirectory `docker/data` such that it is visible for IDEA (e.g. `export MVRS_DSPA_DATA_DIR=file:///dspa19/projects/mvrs-dspa/docker/data` in `.profile`)
1. start IntelliJ IDEA -> "Import Project" (selecting `pom.xml` in `mvrs-dspa`), accepting all defaults. Unfortunately, during the import process the configured run configurations are deleted. To bring them back:
   1. close IDEA
   1. cd to the project directory
   1. enter `git checkout -- .idea/runConfigurations` 
   1. restart IDEA and open the project
   1. confirm that the run configurations (drop down list in upper right of IDEA window) are available (order can differ):
      <img src="https://github.com/rschoening/mvrs-dspa/blob/master/doc/images/idea-run-configurations.png" alt="IDEA run configurations" width="60%"/>

## Setting up the runtime environment
1. make sure that `dockerd` is running
1. cd to the repository: `mvrs-dspa/docker`
1. as privileged user, enter `docker-compose up -d`
1. check that all containers were started successfully: enter `docker-compose ps` The following containers should be listed:
```
Name                    Command                         State  Ports                       
----------------------------------------------------------------------------------------------------------------
docker_elasticsearch_1  /usr/local/bin/docker-entr ...  Up    0.0.0.0:9201->9200/tcp, 9300/tcp                  
docker_exporter_1       /bin/node_exporter              Up    0.0.0.0:9101->9100/tcp                            
docker_grafana_1        /run.sh                         Up    0.0.0.0:3001->3000/tcp                            
docker_jobmanager_1     /docker-entrypoint.sh jobm ...  Up    6123/tcp, 0.0.0.0:8082->8081/tcp, 9249/tcp        
docker_kafka_1          start-kafka.sh                  Up    0.0.0.0:9092->9092/tcp, 9093/tcp                  
docker_kibana_1         /usr/local/bin/kibana-docker    Up    0.0.0.0:5602->5601/tcp                            
docker_prometheus_1     /bin/prometheus --config.f ...  Up    0.0.0.0:9091->9090/tcp                            
docker_taskmanager_1    /docker-entrypoint.sh task ...  Up    6121/tcp, 6122/tcp, 6123/tcp, 8081/tcp, 9249/tcp  
docker_zookeeper_1      /bin/sh -c /usr/sbin/sshd  ...  Up    0.0.0.0:2181->2181/tcp, 22/tcp, 2888/tcp, 3888/tcp
```
   * in case `docker_elasticsearch_1` is not listed:
      1. as su, enter `sysctl -w vm.max_map_count=262144` (see https://www.elastic.co/guide/en/elasticsearch/reference/6.7/vm-max-map-count.html).
      1. `docker-compose down`
      1. `docker-compose up -d`
      1. check again with `docker-compose ps`
5. Import the dashboards in Kibana:
   1. open Kibana in the browser, at http://localhost:5602/
   1. go to `Management`-> `Saved Objects`
   1. import `export.json` from `mvrs-dspa/docker/kibana`
   
      <img src="https://github.com/rschoening/mvrs-dspa/blob/master/doc/images/kibana-saved-objects-import.png" alt="Kibana import objects" width="60%"/>
   1. go to `Index patterns` and *star* one of the listed index patterns. Any will do (otherwise the imported dashboards are not listed)
   
      <img src="https://github.com/rschoening/mvrs-dspa/blob/master/doc/images/kibana-index-patterns-star.png" alt="Kibana import objects" width="60%"/>

### Configuration files for container services
* The Flink containers read the Flink config file from a volume mapped to the host machine: `mvrs-dspa/docker/flink/conf/flink-conf.yaml`
* Prometheus similarly reads its configuration from a volume mapped to a host directory: `mvrs-dspa/docker/prometheus/prometheus.yml`
* A similar mapping could be made for ElasticSearch and Kibana configuration directories (not currently needed)
* The Kafka container allows parameter customization via environment variables set in the `docker-compose.yml` file, which are translated to matching Kafka properties (dropping the `KAFKA_`-prefix and replacing `_` with `.`, e.g. `KAFKA_TRANSACTION_MAX_TIMEOUT_MS` -> `transaction.max.timeout.ms`)

## Running the Flink jobs
### Overview
* The data preparation and analytic jobs can be configured using the [application.conf](https://github.com/rschoening/mvrs-dspa/blob/master/src/main/resources/application.conf) file. The individual settings are explained in that file. The current configuration can be used as-is for the low-volume test datasets (1K). For the high-volume data, the value for `data.speedup-factor` needs to be reduced (to 2500).
* The analytic jobs depend on the data to have previously been loaded using the data preparation jobs. There are no dependencies _between_ analytic jobs for different project tasks.
* Note that restarting the Kafka container deletes the topics. Also, when starting jobs that write to a Kafka topic, the topic is first deleted and recreated if it exists. Consequences are:
  * after starting the docker container for Kafka, the data preparation job that writes events to Kafka must be re-run (the same is _not_ true for the ElasticSearch indices, which are maintained across container restarts in a docker volume)
  * when starting a job that writes to a Kafka topic, there should not be another job running that reads from the same topic, or the deletion will fail. This only affects the two jobs for active post statistics (the first of which writes to Kafka, from which the second reads)
* Most IDEA run configuration set the program argument `local-with-ui` to launch the Flink dashboard UI and publish metrics to Prometheus. This can be removed if multiple jobs should be run simultaneously (to avoid a port binding exception on startup).

### Data preparation
The following two jobs must have been run prior to running any of the analytics jobs.
* The first job (writing static data to ElasticSearch) must be re-run after changing the LSH configuration for the recommendations task (in [application.conf](https://github.com/rschoening/mvrs-dspa/blob/master/src/main/resources/application.conf)), as the MinHash/LSH configuration in the prepared static data and the recommendation job must match for correct results.
* As noted above, the second job (writing the events to Kafka) needs to be re-run after restarting the Kafka container.
* The two jobs terminate in less than a minute total, for the low-volume testdata.

#### Loading static data into ElasticSearch
* Inputs:
  * csv files in testdata directory `tables` (location configured in `application.conf` and environment variable `MVRS_DSPA_DATA_DIR`)
* Outputs:
  * ElasticSearch indexes `mvrs-recommendation-person-features`, `mvrs-recommendation-forum-features`, `mvrs-recommendation-person-minhash`, `mvrs-recommendation-known-persons`, `mvrs-recommendation-lsh-buckets`
* [Execution plan](https://github.com/rschoening/mvrs-dspa/blob/master/doc/plans/load_static_tables.pdf)
* Job class: `org.mvrs.dspa.jobs.preparation.LoadStaticDataJob`
* in IDEA, execute the run configuration `Preparation: load static tables (csv -> ElasticSearch)`
   * The job can be re-run as needed. Output indices are deleted and recreated if they already exist.
* Checking results: the load progress and final result can be observed in Kibana:
   * Document counts on index management page:
     <img src="https://github.com/rschoening/mvrs-dspa/blob/master/doc/images/kibana-index-management.png" alt="Kibana index management" width="60%"/>
   * `Discover` page (make sure to set time range to something like 'Last 15 minutes', as the static data is timestamped with insertion time):
   
     <img src="https://github.com/rschoening/mvrs-dspa/blob/master/doc/images/kibana-discover-static-data-loading.png" alt="Kibana discover - static data" width="60%"/>

#### Writing events to Kafka
* Inputs:
   * csv files in testdata directory `streams` (location configured in `application.conf` and environment variable `MVRS_DSPA_DATA_DIR`)
* Outputs:
   * Event topics in Kafka: `mvrs_comments`, `mvrs_likes`, `mvrs_posts`
* [Execution plan](https://github.com/rschoening/mvrs-dspa/blob/master/doc/plans/write_events_to_kafka.pdf)
* Job class: `org.mvrs.dspa.jobs.preparation.WriteEventsToKafkaJob`
* in IDEA, execute the run configuration `Preparation: load events (csv -> Kafka)`
* Checking results: 
   * Metrics in Flink Dashboard or Prometheus/Grafana
   * Querying partition offsets using the Kafka CLI

##### Notes
  * To simulate out-of-order events, the value for `data.random-delay` in `application.conf` file can be modified prior to loading the events into Kafka. The random delay is used to parameterize a normal distribution of delays, with a mean of 1/4 and standard deviation of 1/2 of the configured value, capping the distribution at that value (see `org.mvrs.dspa.utils.FlinkUtils.getNormalDelayMillis()`)  
  * The speedup factor (`data.speedup-factor` in `application.conf`) is only applied during the analytic jobs, not during data preparation.
  * To allow precise control over the ordering and lateness of events when reading from Kafka, the preparation job uses a single worker, and writes to a single Kafka partition. Doing otherwise would create additional sources of un-ordering that would not allow exact control of lateness/reordering based on defined values for `data.random-delay` and `data.max-out-of-orderness`. See also the discussion for `data.kafka-partition.count` in [application.conf](https://github.com/rschoening/mvrs-dspa/blob/master/src/main/resources/application.conf)

### General observations for analytic tasks
* The analytic jobs apply the speedup factor when reading from Kafka (`application.conf` contains comments on tested speedup factors).
* Most of the analytic jobs print information about late events encountered in the final result to standard output. Late events can be simulated by setting `data.max-out-of-orderness` to a value smaller than `data.random-delay`.
* Contrary to the initial plan, the reply tree is now reconstructed *after* reading from Kafka. Reason: this function was specifically built to run in parallel and to be fault-tolerant, both aspects are no longer relevant for the data loading into Kafka (to ensure defined bounds for out-of-orderness and lateness of events, as noted above). So to keep things interesting, the reply tree is assembled after reading from Kafka, and therefore as part of all analytic jobs.
* ... and interesting it turned out to be, as I realized that the use of union list state for the comment-to-post mapping leads to full replication of the implicitly partitioned operator state on recovery/rescaling. The original plan was to deal with the unbounded state by partitioning it across machines, which breaks down after the first failure (as the function apparently cannot know which mappings it could discard). So this mapping should really be maintained in a central datastore, which would require disassembling the function logic into a subgraph dealing with asynchronous data access to ElasticSearch and persistence of newly found mappings. Being a bit more familiar with Async I/O functions and side outputs by now, this seems feasible though.

### Active post statistics
#### Calculating post statistics
* Inputs
   * Kafka topics: `mvrs_comments`, `mvrs_likes`, `mvrs_posts`
* Outputs
   * Kafka topic: `mvrs_poststatistics`
   * ElasticSearch index: `mvrs-active-post-statistics-postinfos`
* [Execution plan](https://github.com/rschoening/mvrs-dspa/blob/master/doc/plans/poststatistics_to_kafka.pdf)
* Job class: `org.mvrs.dspa.jobs.activeposts.ActivePostStatisticsJob`
* in IDEA, execute the run configuration `Task 1.1: active post statistics (Kafka -> Kafka, post info: Kafka -> ElasticSearch)`
* Checking results:
  * View the incoming documents in `mvrs-active-post-statistics-postinfos` using the `Discover` page in Kibana (setting the time range to the start event time of the stream, i.e. February 2012 for the low-volume stream).
  * run the next job to write the statistics to ElasticSearch.
  * Integration tests in `jobs.activeposts.PostStatisticsFunctionITSuite`

#### Notes
* Job-specific configuration parameters are defined in `jobs.active-post-statistics` ([application.conf](https://github.com/rschoening/mvrs-dspa/blob/master/src/main/resources/application.conf))
* This job uses the `EXACTLY_ONCE` semantic for writing to Kafka. However not all the relevant Kafka settings have yet been revised and adjusted for this. The necessary Kafka configuration parameters can be set in the `docker-compose.yml` file (in the form `KAFKA_TRANSACTION_MAX_TIMEOUT_MS : 3600000`)

#### Writing post statistics results to ElasticSearch index
* Inputs (generated by previous job)
  * Kafka topic: `mvrs_poststatistics`
  * ElasticSearch index: `mvrs-active-post-statistics-postinfos`
* Outputs
  * ElasticSearch index: `mvrs-active-post-statistics`
* [Execution plan](https://github.com/rschoening/mvrs-dspa/blob/master/doc/plans/poststatistics_to_es.pdf)
* Job class: `org.mvrs.dspa.jobs.activeposts.WriteActivePostStatisticsToElasticSearchJob`
* In IDEA, execute the run configuration `Task 1.2: active post statistics - (Kafka -> ElasticSearch) [NO UI]`
  * The run configuration does _not_ set the argument `local-with-ui`, to allow for parallel execution with the previous task on local machine/minicluster.
* Checking results:
   * Kibana dashboard: [\[DSPA\] Active post statistics](http://localhost:5602/app/kibana#/dashboard/a0af2f50-4f0f-11e9-acde-e1c8a6292b89)
      * Note that the "New posts per hour" gauge on the right is based on the `mvrs-active-post-statistics-postinfos` index populated by the previous job. All other visualizations on this dashboard are based on `mvrs-active-post-statistics` written by this job.
      * The table on the left aggregates all statistics records for a post within the time range of the dashboard, and shows the maximum values observed in any of the contained 12-hour windows, sorting by the maximum number of distinct persons. The raw statistics documents in `mvrs-active-post-statistics` can be viewed on `Discover` (top left, below Kibana logo).
      * Make sure to set the time range (upper right) to the beginning of the stream (February 2012 for the low volume stream). A period of one week is recommended.
      * When clicking on a tag in the tag cloud (or some other parts of visualizations), a persistent filter is defined. These filters are displayed on the top-left of the dashboard and can be removed again.

#### Notes
* Job-specific configuration parameters are defined in `jobs.active-post-statistics` ([application.conf](https://github.com/rschoening/mvrs-dspa/blob/master/src/main/resources/application.conf))
* The purpose of the lookup of post/forum information in ElasticSearch is to avoid carrying the post information along with the statistics stream; instead that stream is kept lean, and enriched later when writing to ElasticSearch for analysis. An alternative strategy would be to connect/join the statistics stream to the enriched post stream prior to writing the statistics to ElasticSearch. Since having the post content for search/display in Kibana is anyway useful, and the cache-enabled Async I/O queries to ElasticSearch perform quite well, the current approach was preferred.

### Recommendations
* Inputs (created by data preparation jobs, which have to be run before, see above): 
   * Event topics in Kafka: `mvrs_comments`, `mvrs_likes`, `mvrs_posts`
   * ElasticSearch indexes with static data: `mvrs-recommendation-person-features`, `mvrs-recommendation-forum-features`, `mvrs-recommendation-person-minhash`, `mvrs-recommendation-known-persons`, `mvrs-recommendation-lsh-buckets`
* Outputs (re-generated automatically when the job starts):
   * ElasticSearch index with recommendation documents: `mvrs-recommendations`
   * ElasticSearch index with post features: `mvrs-recommendation-post-features`
* [Execution plan](https://github.com/rschoening/mvrs-dspa/blob/master/doc/plans/recommendations.pdf)
* Job class: `org.mvrs.dspa.jobs.recommendations.RecommendationsJob`
* In IDEA, execute the run configuration `Task 2: user recommendations (Kafka -> ElasticSearch)` 
* Checking results:
   * Kibana dashboard: [\[DSPA\] Recommendations](http://localhost:5602/app/kibana#/dashboard/7c230710-6855-11e9-9ba6-39d0e49adb7a)
      * Make sure to set the time range (upper right) to the beginning of the stream (February 2012 for the low volume stream). All visualizations in Kibana depend on this time range.
      * The recommendation documents are shown on the left and can be investigated in detail (expanding the document tree, displaying as JSON etc.). Note that old recommendations for a given person are continuously replaced by current ones (upserts by person id). In a given time range, the number of documents will therefore diminish over time. Use the right-arrow next to the time range display to advance along with the tail of the stream. To look at a document in detail, it may be necessary to stop the stream, otherwise the document may be deleted quickly.
      <img src="https://github.com/rschoening/mvrs-dspa/blob/master/doc/images/kibana-dashboard-recommendations.png" alt="Kibana dashboard: recommendations" width="60%"/>
   * Unit and integration tests in test package `jobs.recommendations`

#### Notes
* Job-specific configuration parameters are defined in `jobs.recommendation` ([application.conf](https://github.com/rschoening/mvrs-dspa/blob/master/src/main/resources/application.conf))

### Unusual activity detection
* Inputs
   * Kafka topics: `mvrs_comments`, `mvrs_likes`, `mvrs_posts`
   * Control parameter file: path set by `jobs.activity-detection.cluster-parameter-file-path` in [application.conf](https://github.com/rschoening/mvrs-dspa/blob/master/src/main/resources/application.conf)
* Outputs
   * ElasticSearch indexes:
      * Classification results: `mvrs-activity-classification`
      * Cluster metadata: `mvrs-activity-cluster-metadata`
      * Control parameter parse error messages: path text file output directory set by `jobs.activity-detection.cluster-parameter-file-parse-errors-path` in [application.conf](https://github.com/rschoening/mvrs-dspa/blob/master/src/main/resources/application.conf)
* [Execution plan](https://github.com/rschoening/mvrs-dspa/blob/master/doc/plans/unusual_activity.pdf)
* Job class: `org.mvrs.dspa.jobs.clustering.UnusualActivityDetectionJob`
* In IDEA, execute the run configuration `Task 3: unusual activity detection (Kafka -> ElasticSearch)`
* Checking results:
   * Kibana dashboard: [\[DSPA\] Unusual activity detection](http://localhost:5602/app/kibana#/dashboard/83a893d0-6989-11e9-ba9d-bb8bdc29536e)
      * Make sure to set the time range (upper right) to the beginning of the stream (February 2012 for the low volume stream). A period of one week is recommended.
      * The diagram of cluster changes is meant as an example, the displayed variables would have to be more carefully defined. But since the job anyway focuses on the streaming mechanisms and a decent streaming K-means implementation, but not on a valid approach from a data science perspective, not much time was spent on this.
      * The cluster models and model change information can be investigated in the raw documents shown on the right.
      * The cluster metadata graph can have gaps since the used Kibana visualization does not interpolate across buckets with nodata (which may result due to extending windows). With a time range of 7 days, this should not happen. Different, more advanced visualizations are available in Kibana that interpolate across empty buckets. 
   * Unit tests for the K-Means implementation, cluster model management and control parameter parsing in test package `jobs.clustering`

#### Notes
* Job-specific configuration parameters are defined in `jobs.activity-detection` ([application.conf](https://github.com/rschoening/mvrs-dspa/blob/master/src/main/resources/application.conf))
* The labelling of clusters based on the control stream is currently only applied during the next cluster update. It would make more sense to apply these labels immediately for subsequent classifications. This would require connecting to the control stream twice, both on the cluster model update stream (for `k` and `decay`) and on the event classification stream (for the labels).

## Solution overview
### Package structure
```
└─ src                                      
│  └─ main                                  
│  │  └─ resources                          
│  │  │  └─ application.conf                │ configuration file, with documentation on all settings
│  │  └─ scala                              │
│  │     └─ org.mvrs.dspa                   │ root package of Scala solution
│  │        └─ db                           │ package with data access types for ElasticSearch 
│  │        │    ElasticSearchIndexes.scala │ - static registry of ElasticSearch indices
│  │        │                               │ - for each index there is a gateway class performing the schema-dependent
│  │        │                               │   operations required for that index (index creation, document creation, 
│  │        │                               │   Flink sink creation, AsyncI/O-function creation)
│  │        └─ functions                    │ package for stream functions that are not strictly tied to one job
│  │        └─ jobs                         │ package with job implementations 
│  │        │  └─ activeposts               │ package for active post statistics jobs
│  │        │  └─ clustering                │ package for unusual activity detection job
│  │        │  └─ preparation               │ package for data preparation jobs (static tables, events)
│  │        │  └─ recommendations           │ package for user recommendations job
│  │        └─ model                        │ package for domain model types
│  │        └─ streams                      │ package for input streams (csv, Kafka)
│  │        │    package.scala              │ methods for reading input streams (csv or Kafka, comments raw or resolved)
│  │        │    KafkaTopics.scala          │ static registry of Kafka topics
│  │        └─ utils                        │ (more or less) generic utilities 
│  │        Settings.scala                  │ Object for accessing settings in application.conf
│  └─ test                                  │ tests and test resources
│     └─ resources                          │
│     │  └─ resources                       │
│     │     └─ streams                      │ directory with reduced streaming test data files (csv)
│     └─ scala                              │
│     └─ categories                         │ package for definition of test categories
│     └─ db                                 │ integration tests (ignored) for interaction with ElasticSearch
│     └─ functions                          │ unit and integration tests for functions package
│     └─ jobs                               │ unit and integration tests for the analytic tasks 
│     └─ streams                            │ unit and integration tests for the primary input streams
│     └─ utils                              │ tests and trials for utilities
└─ target                                   │
   └─ site                                  │
   │  └─ scaladoc                           │ scaladoc site generated with mvn scala:doc
   │        index.html                      │
   │  mvrs-dspa-1.0.jar                     │ the fat jar that can be submitted to a Flink cluster, built by 'mvn package'
```

### Configuration
* `mvrs-dspa/src/main/resources/application.conf`
  * Based on https://github.com/lightbend/config/blob/master/README.md
  * Settings are documented in the file
  * Settings can be overridden at runtime using various mechanisms (see https://github.com/lightbend/config/blob/master/README.md#overview) 

### Scaladoc
* can be generated with `mvn scala:doc`
* written to `mvrs-dspa/target/site/scaladoc`

### Unit tests
* The plan was to have significantly more tests than what are there now. But time got short and it turned out that analysis of the end-to-end jobs (using metrics, Kibana, the progress monitor etc.) was more important, yielding unexpected findings I would never have looked for specifically in unit tests.
* Unit tests:
  * Scalatest
  * Naming convention: `...TestSuite`
* Integration tests involving the Flink Minicluster or external systems:
  * JUnit
  * Naming convention: `...ITSuite`
  * Most integration tests avoid external dependencies. Those that do interact with ElasticSearch or Kafka are ignored, and are only executed when invoked invidiually.
* IDEA run configurations: 
  * `ALL: integration tests (junit)`
  * `ALL: unit tests (scalatest)`

### ElasticSearch indexes
* The mappings, documents and queries used for these indexes are defined in the gateway classes in the `db` package. These classes can be accessed from the static registry `ElasticSearchIndexes.scala`.

## Addresses:
* Flink minicluster dashboard (when running jobs using the provided run configurations in IDEA): http://localhost:8081
* Flink "remote" cluster dashboard (docker): http://localhost:8082
* Kibana (docker): http://localhost:5602
  * Active post statistics: http://localhost:5602/app/kibana#/dashboard/a0af2f50-4f0f-11e9-acde-e1c8a6292b89
  * Recommendations: http://localhost:5602/app/kibana#/dashboard/7c230710-6855-11e9-9ba6-39d0e49adb7a
  * Activity detection: http://localhost:5602/app/kibana#/dashboard/83a893d0-6989-11e9-ba9d-bb8bdc29536e
* Prometheus (docker): http://localhost:9091
* Grafana (docker): http://localhost:3001 (no dashboards delivered as part of solution; initial login with admin/admin, then change pwd)
* ElasticSearch (docker, for status check): http://localhost:9201

## Troubleshooting
* Some possible problems and solutions are listed [here](https://github.com/rschoening/mvrs-dspa/blob/master/doc/Troubleshooting.md)
