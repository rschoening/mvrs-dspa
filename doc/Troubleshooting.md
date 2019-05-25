## Troubleshooting

### When running `docker-compose up`:
  * `ERROR: for docker_exporter_1  Cannot start service exporter: driver failed programming external connectivity on endpoint docker_exporter_1 (c1b1acc38d051b138707fbae6e323641332e3f659145eef51ea5764b2a3953e7): Error starting userland proxy: mkdir /port/tcp:0.0.0.0:9101:tcp:172.18.0.2:9100: input/output error`
    * Sometimes observed on Windows on first `docker-compose up`in session.
    * Solution: restart docker, try again
  * on linux: `ERROR: Couldn't connect to Docker daemon at http+docker://localhost - is it running?`
    * start `dockerd` as su
* failure to start container `elasticsearch` (best diagnosed when starting that container individually, using 
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
  * Solution: enter as su: `sysctl -w vm.max_map_count=262144` (see https://www.elastic.co/guide/en/elasticsearch/reference/6.7/vm-max-map-count.html)

### When starting a Flink Job
* `Exception in thread "main" java.util.concurrent.ExecutionException: org.apache.kafka.common.errors.TimeoutException: Timed out waiting for a node assignment.`
  * Probable cause: Kafka not running.
  * Solution: start with `docker-compose up` in directory `docker`
* When using a IDEA run configuration that launches the Flink Dashboard for the local minicluster: `Exception in thread "main" java.net.BindException: Address already in use: bind
	at sun.nio.ch.Net.bind0(Native Method)
	at sun.nio.ch.Net.bind(Net.java:433)
	at sun.nio.ch.Net.bind(Net.java:425)
	at sun.nio.ch.ServerSocketChannelImpl.bind(ServerSocketChannelImpl.java:223)
	at org.apache.flink.shaded.netty4.io.netty.channel.socket.nio.NioServerSocketChannel.doBind(NioServerSocketChannel.java:128)`
  * Probable causes: either another running Flink job has already started the dashboard (on the same port), or the dashboard is still open in the browser (in some situations this also keeps the port open)
  * Solution: shut down the other job and/or the browser window, or change the run configuration to start the job without the `local-with-ui` program argument.

### When starting the Flink job for Unusual Activity Detection
* `Exception in thread "main" org.apache.flink.runtime.client.JobExecutionException: Job execution failed. ... Caused by: java.io.FileNotFoundException: The provided file path file:/c:/data/dspa/project/10k-users-sorted/mvrs/activity-classification.txt does not exist.`
  * copy parameter file `./docker/data/mvrs/activity-classification.txt` to a subdirectory `mvrs` in the data directory indicated by the environment variable `MVRS_DSPA_DATA_DIR` (if using a non-default location the data directory)
