package org.mvrs.dspa.utils

import java.util.concurrent.TimeUnit
import java.util.{Optional, Properties}

import com.codahale.metrics.SlidingWindowReservoir
import javax.annotation.Nonnegative
import org.apache.flink.api.common.serialization.{DeserializationSchema, SerializationSchema, TypeInformationSerializationSchema}
import org.apache.flink.api.common.state.ListState
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.metrics.ScalaGauge
import org.apache.flink.configuration.{ConfigConstants, Configuration, WebOptions}
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper
import org.apache.flink.metrics.MetricGroup
import org.apache.flink.streaming.api.datastream.{AsyncDataStream, DataStreamSink}
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.async.AsyncFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper
import org.mvrs.dspa.functions.ProgressMonitorFunction
import org.mvrs.dspa.functions.ProgressMonitorFunction.ProgressInfo
import org.mvrs.dspa.utils.kafka.KafkaTopic

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

/**
  * Utilities for Flink
  */
object FlinkUtils {
  /**
    * Convert from flink's common Time to the old streaming Time still used for windows
    *
    * @param time time instance to convert
    * @return streaming time instance
    */
  def convert(time: org.apache.flink.api.common.time.Time): org.apache.flink.streaming.api.windowing.time.Time =
    org.apache.flink.streaming.api.windowing.time.Time.milliseconds(time.toMilliseconds)

  /**
    * Helper for creation asynchronous data streams from Scala
    *
    * @param dataStream    The input stream
    * @param asyncFunction The async IO function
    * @param timeout       for the asynchronous operation to complete
    * @param timeUnit      of the given timeout
    * @param capacity      The max number of async i/o operation that can be triggered
    * @param unordered     Indicates if the output records may be reordered, or if the input order should be maintained
    * @tparam IN  Type of input record
    * @tparam OUT Type of output record
    * @return output stream
    */
  def asyncStream[IN, OUT](dataStream: DataStream[IN], asyncFunction: AsyncFunction[IN, OUT], unordered: Boolean = true)
                          (implicit timeout: Long = 5, timeUnit: TimeUnit = TimeUnit.SECONDS, capacity: Int = 5): DataStream[OUT] = {
    new DataStream[OUT](
      if (unordered)
        AsyncDataStream.unorderedWait(dataStream.javaStream, asyncFunction, timeout, timeUnit, capacity)
      else
        AsyncDataStream.orderedWait(dataStream.javaStream, asyncFunction, timeout, timeUnit, capacity)
    )
  }

  /**
    * Calculates time-to-live based on a time in event time, a replay speedup factor and a minimum time (in processing time) to cover late events
    *
    * @param time              Time-to-live in event time
    * @param speedupFactor     Replay speedup factor - if 0 (infinite speedup, i.e. read as fast as possible): the minimum time is returned
    * @param minimumTimeMillis The minimum time-to-live in processing time (milliseconds). Without this lower bound,
    *                          The time-to-live in event time can get very small with high speedup factors, with the
    *                          effect that even slightly late events miss the state that has been set up for them.
    * @return The time-to-live in processing time
    */
  def getTtl(time: Time, speedupFactor: Double = 0, minimumTimeMillis: Long = 60000): Time =
    Time.of(
      if (speedupFactor > 0)
        math.max(time.toMilliseconds / speedupFactor, minimumTimeMillis).toLong
      else
        minimumTimeMillis,
      TimeUnit.MILLISECONDS
    ) // TODO actual replay speed may be lower due to backpressure/slow source -> apply some factor to account for that? Or just let the caller reduce the speedup factor for this?

  /**
    * Create the stream execution environment, optionally with enabled metrics and web UI on the local minicluster
    *
    * @param localWithUI Indicates that the local minicluster environment with enabled metrics and web UI should be
    *                    used. If ```false``` the default environment is returned, which may be local or remote.
    * @return The execution environment
    */
  def createStreamExecutionEnvironment(localWithUI: Boolean = false): StreamExecutionEnvironment = {
    if (localWithUI) {
      val config = new Configuration

      config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true)
      config.setString(WebOptions.LOG_PATH, "./data/flinkui.log") // TODO revise

      config.setString("metrics.reporters", "prometheus")
      config.setString("metrics.reporter.prometheus.class", "org.apache.flink.metrics.prometheus.PrometheusReporter")

      StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config)
    }
    else StreamExecutionEnvironment.getExecutionEnvironment
  }

  /**
    * Create a remote stream execution environment
    *
    * @param host     The hostname of the Flink cluster
    * @param port     The port of the Flink cluster
    * @param jarFiles The JAR files with code that needs to be shipped to the cluster. If the program uses
    *                 user-defined functions, user-defined input formats, or any libraries, those must be provided
    *                 in the JAR files.
    * @return The execution environment
    * @note If a locally assembled job graph is submitted via this environment, all external dependencies
    *       accessed by the job (paths, ports, addresses) must match between the local machine and the remote environment.
    */
  def createRemoteStreamExecutionEnvironment(host: String, port: Int, jarFiles: String*): StreamExecutionEnvironment =
    StreamExecutionEnvironment.createRemoteEnvironment(host: String, port, jarFiles: _*)

  /**
    * Creates the type info serialization schema for a given type
    *
    * @param env The stream execution environment.
    * @tparam T The type, for which TypeInformation must be available
    * @return The serialization schema
    */
  def createTypeInfoSerializationSchema[T: TypeInformation](implicit env: StreamExecutionEnvironment): TypeInformationSerializationSchema[T] =
    new TypeInformationSerializationSchema[T](createTypeInformation[T], env.getConfig)

  /**
    * Create the batch execution environment, optionally with the web UI launched for the local minicluster
    *
    * @param localWithUI Indicates that the local minicluster environment with enabled web UI should be
    *                    used. If ```false``` the default environment is returned, which may be local or remote.
    * @return The execution environment
    */
  def createBatchExecutionEnvironment(localWithUI: Boolean = false): ExecutionEnvironment =
    if (localWithUI) ExecutionEnvironment.createLocalEnvironmentWithWebUI()
    else ExecutionEnvironment.getExecutionEnvironment

  /**
    * Create a remote batch execution environment
    *
    * @param host     The hostname of the Flink cluster
    * @param port     The port of the Flink cluster
    * @param jarFiles The JAR files with code that needs to be shipped to the cluster. If the program uses
    *                 user-defined functions, user-defined input formats, or any libraries, those must be provided
    *                 in the JAR files.
    * @return The execution environment
    * @note If a locally assembled job graph is submitted via this environment, all external dependencies
    *       accessed by the job (paths, ports, addresses) must match between the local machine and the remote environment.
    */
  def createRemoteBatchExecutionEnvironment(host: String, port: Int, jarFiles: String*): ExecutionEnvironment =
    ExecutionEnvironment.createRemoteEnvironment(host: String, port, jarFiles: _*)

  /**
    * Shorthand for reading a csv file in the format of the project test data, and returning a DataSet
    *
    * @param path            to the csv file
    * @param env             The batch execution environment
    * @param typeInformation The type information for the output type
    * @tparam T The type of the DataSet records
    * @return DataSet
    */
  def readCsv[T: ClassTag](path: String)
                          (implicit env: ExecutionEnvironment, typeInformation: TypeInformation[T]): DataSet[T] =
    env.readCsvFile[T](path, fieldDelimiter = "|", ignoreFirstLine = true)
      .name(path)

  /**
    * Shorthand for creating a new Kafka topic and writing a given stream to it.
    *
    * @param stream            The stream to write to Kafka
    * @param topic             The topic
    * @param numPartitions     The number of partitions to create for the topic
    * @param partitioner       The optional partitioner for writing to the Topic. If no partitioner is specified,
    *                          Kafka's default partitioner (round-robin) is used.
    * @param replicationFactor The replication factor for the topic
    * @param semantic          The write semantic used by Flink
    * @param parallelism       Optional override of the default parallelism
    * @tparam E The record type
    * @return The stream sink
    */
  def writeToNewKafkaTopic[E](stream: DataStream[E],
                              topic: KafkaTopic[E],
                              numPartitions: Int = 5,
                              partitioner: Option[FlinkKafkaPartitioner[E]] = None,
                              replicationFactor: Short = 1,
                              semantic: FlinkKafkaProducer.Semantic = Semantic.AT_LEAST_ONCE,
                              parallelism: Option[Int] = None): DataStreamSink[E] = {
    // (re)create the topic
    topic.create(numPartitions, replicationFactor, overwrite = true)

    // add the producer/sink
    val sink = stream
      .addSink(topic.producer(partitioner, semantic))
      .name(s"Kafka: ${topic.name}")

    parallelism.foreach(sink.setParallelism) // apply parallelism if not None

    sink
  }

  /**
    * Creates a Flink Kafka producer.
    *
    * @param topicName             The topic name
    * @param bootstrapServers      The bootstrap servers to connect to
    * @param serializationSchema   The serialization schema
    * @param partitioner           The optional partitioner for writing to the Topic. If no partitioner is specified, Kafka's
    *                              default partitioner (round-robin) is used.
    * @param semantic              The write semantic used by Flink
    * @param kafkaProducerPoolSize The Kafka producer pool size for writing with EXACTLY_ONCE semantic
    * @param writeTimestampToKafka Indicates if timestamps should be explicitly written to Kafka
    * @tparam T The record type to produce
    * @return The Flink Kafka producer
    */
  def createKafkaProducer[T](topicName: String,
                             bootstrapServers: String,
                             serializationSchema: SerializationSchema[T],
                             partitioner: Option[FlinkKafkaPartitioner[T]],
                             semantic: FlinkKafkaProducer.Semantic = Semantic.AT_LEAST_ONCE,
                             kafkaProducerPoolSize: Int = FlinkKafkaProducer.DEFAULT_KAFKA_PRODUCERS_POOL_SIZE,
                             writeTimestampToKafka: Boolean = false): FlinkKafkaProducer[T] = {
    // if no partitioner is specified, use the default Kafka partitioner (round-robin) instead of the FlinkFixedPartitioner
    val customPartitioner: Optional[FlinkKafkaPartitioner[T]] = Optional.ofNullable(partitioner.orNull)

    val producer = new FlinkKafkaProducer[T](
      topicName,
      new KeyedSerializationSchemaWrapper(serializationSchema),
      kafka.connectionProperties(bootstrapServers),
      customPartitioner,
      semantic,
      kafkaProducerPoolSize
    )

    // NOTE: EXACTLY_ONCE requires a larger-than-default value for transaction.max.timeout.ms (-> 1h, 3600000)
    //       (set in docker-compose.yaml)

    producer.setWriteTimestampToKafka(writeTimestampToKafka)

    producer
  }

  /**
    * Creates a Flink Kafka consumer
    *
    * @param topicName                  The topic name
    * @param props                      The consumer properties for connecting to Kafka
    * @param deserializationSchema      The deserialization schema
    * @param commitOffsetsOnCheckpoints Specifies whether or not the consumer should commit offsets back to Kafka on
    *                                   checkpoints. This setting will only have effect if checkpointing is enabled for
    *                                   the job. If checkpointing isn't enabled, only the "enable.auto.commit"
    *                                   property settings will be used.
    * @tparam T The record type
    * @return The Flink Kafka consumer
    */
  def createKafkaConsumer[T](topicName: String,
                             props: Properties,
                             deserializationSchema: DeserializationSchema[T],
                             commitOffsetsOnCheckpoints: Boolean): FlinkKafkaConsumer[T] = {
    val consumer = new FlinkKafkaConsumer[T](topicName, deserializationSchema, props)
    consumer.setStartFromEarliest() // by default, start from earliest. Caller can override on returned instance
    consumer.setCommitOffsetsOnCheckpoints(commitOffsetsOnCheckpoints)
    consumer
  }

  /**
    * Creates a bounded-out-of-orderness timestamp extractor
    *
    * @param maxOutOfOrderness The (fixed) interval between the maximum timestamp seen in the records
    *                          and that of the watermark to be emitted.
    * @param extract           The function to extract the timestamp from a record
    * @tparam T The record type
    * @return The timestamp extractor
    */
  def timeStampExtractor[T](maxOutOfOrderness: Time, extract: T => Long): AssignerWithPeriodicWatermarks[T] = {
    new BoundedOutOfOrdernessTimestampExtractor[T](
      org.apache.flink.streaming.api.windowing.time.Time.milliseconds(maxOutOfOrderness.toMilliseconds)) {
      override def extractTimestamp(element: T): Long = extract(element)
    }
  }

  /**
    * Converts from a Flink ListState to a scala Seq
    *
    * @param listState The ListState instance
    * @tparam T The record type
    * @return Seq of T
    */
  def toSeq[T](listState: ListState[T]): Seq[T] =
    listState.get() match {
      case null => Nil // no state -> null!
      case xs@_ => xs.asScala.toSeq
    }

  /**
    * Generates random delay values, with a given mean and standard deviation, and not exceeding a given maximum value.
    * Default values are chosen for a not too unrealistic skewed delay distribution (maximum value = mean + 1.5 stdev)
    *
    * @param rand                     the random number generator
    * @param maximumDelayMilliseconds the maximum delay value.
    * @param mean                     the mean delay value
    * @param standardDeviation        the standard deviation
    * @return
    */
  def getNormalDelayMillis(rand: scala.util.Random,
                           @Nonnegative maximumDelayMilliseconds: Long)
                          (mean: Double = maximumDelayMilliseconds / 4.0,
                           @Nonnegative standardDeviation: Double = maximumDelayMilliseconds / 2.0): Long = {
    var delay = -1L

    while (delay < 0 || delay > maximumDelayMilliseconds) {
      delay = (rand.nextGaussian * standardDeviation + mean).toLong
    }
    delay
  }

  /**
    * Creates a histogram metric
    *
    * @param name                   metric name
    * @param group                  metric group
    * @param slidingWindowReservoir the number of measurements to store
    * @return the histogram metric
    */
  def histogramMetric(name: String,
                      group: MetricGroup,
                      slidingWindowReservoir: Int = 500): DropwizardHistogramWrapper =
    group.histogram(name, new DropwizardHistogramWrapper(new com.codahale.metrics.Histogram(new SlidingWindowReservoir(slidingWindowReservoir))))

  /**
    * Creates a gauge metric
    *
    * @param name     metric name
    * @param group    metric group
    * @param getValue the function to get the gauge value
    * @tparam T type of gauge value
    * @return gauge metric
    */
  def gaugeMetric[T](name: String,
                     group: MetricGroup,
                     getValue: () => T): ScalaGauge[T] =
    group.gauge[T, ScalaGauge[T]](name, ScalaGauge[T](() => getValue()))

  /**
    * Prints the current streaming job execution plan to the default output.
    *
    * @param env The stream execution environment.
    */
  def printExecutionPlan()(implicit env: StreamExecutionEnvironment): Unit = {
    println("\nexecution plan:")
    println(env.getExecutionPlan)
    println()
  }

  /**
    * Prints the current batch job execution plan to the default output.
    *
    * @param env The batch execution environment.
    */
  def printBatchExecutionPlan()(implicit env: ExecutionEnvironment): Unit = {
    println("\nexecution plan:")
    println(env.getExecutionPlan)
    println()
  }

  /**
    * Adds a progress monitor that emits progress information for specific filter conditions to a defined write target.
    *
    * @param stream The stream to monitor
    * @param filter The condition under which to emit detailed progress information for an element
    * @param write  The write target (default: println)
    * @param prefix Optional prefix to prepend to print output
    * @param name   The operator name
    * @tparam I The record type
    * @return The complete and untransformed input stream
    */
  def addProgressMonitor[I: TypeInformation](stream: DataStream[I],
                                             write: String => Unit = println(_),
                                             prefix: String = "",
                                             name: String = "Progress monitor")
                                            (filter: (I, ProgressInfo) => Boolean): DataStream[I] =
    stream.process(new ProgressMonitorFunction())
      .map { t: (I, ProgressInfo) => {
        if (filter(t._1, t._2)) {

          if (t._2.elementCount == 1 && t._2.subtask == 0) write(ProgressInfo.getSchemaInfo)

          write(
            if (prefix != null && prefix.length > 0) s"$prefix: ${t._2.toString}"
            else t._2.toString
          )
        }

        t._1
      }
      }.name(name)

}
