# mvrs-dspa

* prerequisites:
  * docker
  * docker compose
  * maven
  * IntelliJ IDEA with Scala plugin
  
* git clone https://github.com/rschoening/mvrs-dspa.git
* cd to mvrs-dsp
* mvn package
* start IDEA -> "Import Project from Maven" (pom.xml in mvrs-dspa)
  * enable "add dependencies for IDEA"
  * select first module only in list (unselect second)
  
  
TODO
* run configurations (in git repo) apparently get deleted on mvn project import
* screenshots for import options
* docker not yet working in VM, can't test
* where to put data/set env variable

NOTE
* if bind address error occurs when starting job that starts the web UI, then check if the flink dashboard is still open in a browser window. The client keeps the port open. 
* Unusual activity detection: cluster metadata graph can have gaps since that visualization does not interpolate across buckets with nodata (which may result due to extending windows)
