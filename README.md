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
