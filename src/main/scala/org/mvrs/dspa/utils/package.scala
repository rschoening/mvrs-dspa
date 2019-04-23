package org.mvrs.dspa

import java.nio.file.Paths
import java.time.{Instant, LocalDateTime, ZoneId}
import java.util.concurrent.TimeUnit
import java.util.{Optional, Properties}

import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema
import org.apache.flink.api.common.state.ListState
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.datastream.AsyncDataStream
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.async.AsyncFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
// import org.apache.flink.streaming.api.windowing.time.Time

import scala.reflect.ClassTag
//import org.apache.avro.{Schema, SchemaBuilder}
//import org.apache.flink.formats.avro.typeutils.AvroSerializer
//import org.apache.flink.formats.avro.{AvroDeserializationSchema, AvroRowSerializationSchema}
//import org.apache.flink.types.Row
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.commons.lang3.time.DurationFormatUtils
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.kafka.clients.producer.ProducerConfig

import scala.collection.JavaConverters._

// NOTE this will be refactored, currently a mixed bag
package object utils {

  private val dateFormat = ThreadLocal.withInitial[SimpleDateFormat](() => new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))

  /**
    * Calculates time-to-live based on a time in event time, a replay speedup factor and a minimum time (in processing time) to cover late events
    *
    * @param time              time-to-live in event time
    * @param speedupFactor     replay speedup factor - if 0 (infinite speedup, i.e. read as fast as possible): the minimum time is returned
    * @param minimumTimeMillis the minimum time-to-live in processing time (milliseconds). Without this lower bound,
    *                          the time-to-live in event time can get very small with high speedup factors, with the
    *                          effect that even slightly late events miss the state that has been set up for them.
    * @return the time-to-live in processing time
    */
  def getTtl(time: Time, speedupFactor: Int = 0, minimumTimeMillis: Long = 60000): Time =
    Time.of(
      if (speedupFactor > 0)
        math.max(time.toMilliseconds.toDouble / speedupFactor, minimumTimeMillis).toLong
      else
        minimumTimeMillis,
      TimeUnit.MILLISECONDS
    )

  /**
    * Convert from flink's common Time to the old streaming Time still used for windows
    *
    * @param time time instance to convert
    * @return streaming time instance
    */
  def convert(time: org.apache.flink.api.common.time.Time): org.apache.flink.streaming.api.windowing.time.Time =
    org.apache.flink.streaming.api.windowing.time.Time.milliseconds(time.toMilliseconds)

  def toDateTime(millis: Long): LocalDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneId.of("UTC"))

  def toDate(millis: Long): Date = Date.from(Instant.ofEpochMilli(millis))

  /** @return The filesystem path of the given resource */
  def fsPath(resource: String): String =
    Paths.get(getClass.getResource(resource).toURI).toString

  def getNormalDelayMillis(rand: scala.util.Random, maximumDelayMilliseconds: Long): Long = {
    var delay = -1L
    val x = maximumDelayMilliseconds / 2
    while (delay < 0 || delay > maximumDelayMilliseconds) {
      delay = (rand.nextGaussian * x).toLong + x
    }
    delay
  }

  def asyncStream[IN, OUT](dataStream: DataStream[IN], asyncFunction: AsyncFunction[IN, OUT])
                          (implicit timeout: Long = 5, timeUnit: TimeUnit = TimeUnit.SECONDS, capacity: Int = 5): DataStream[OUT] = {
    new DataStream[OUT](
      AsyncDataStream.unorderedWait(dataStream.javaStream, asyncFunction, timeout, timeUnit, capacity)
    )
  }

  def createBatchExecutionEnvironment(localWithUI: Boolean = false): ExecutionEnvironment =
    if (localWithUI) ExecutionEnvironment.createLocalEnvironmentWithWebUI()
    else ExecutionEnvironment.getExecutionEnvironment

  def createStreamExecutionEnvironment(localWithUI: Boolean = false): StreamExecutionEnvironment = {
    // TODO include metrics always? or rely on flink config when running in cluster?
    if (localWithUI) {
      val config = new Configuration
      config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true)
      config.setString("metrics.reporters", "prometheus")
      config.setString("metrics.reporter.prometheus.class", "org.apache.flink.metrics.prometheus.PrometheusReporter")

      StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config)
    }
    else StreamExecutionEnvironment.getExecutionEnvironment
  }

  def formatTimestamp(timestamp: Long): String = dateFormat.get.format(new Date(timestamp))

  def formatDuration(millis: Long): String = DurationFormatUtils.formatDuration(millis, "HH:mm:ss,SSS")

  def readCsv[T: ClassTag](path: String)(implicit env: ExecutionEnvironment, typeInformation: TypeInformation[T]): DataSet[T] =
    env.readCsvFile[T](path, fieldDelimiter = "|", ignoreFirstLine = true)

  def createKafkaProducer[T](topicId: String,
                             bootstrapServers: String,
                             typeInfo: TypeInformation[T],
                             partitioner: Option[FlinkKafkaPartitioner[T]] = None)
                            (implicit env: StreamExecutionEnvironment): FlinkKafkaProducer[T] = {
    val props = new Properties()
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)

    // if no partitioner is specified, use the default Kafka partitioner (round-robin) instead of the FlinkFixedPartitioner
    val part: Optional[FlinkKafkaPartitioner[T]] = Optional.ofNullable(partitioner.orNull)

    val producer = new FlinkKafkaProducer[T](topicId, // target topic
      new TypeInformationSerializationSchema[T](typeInfo, env.getConfig),
      props, part)

    // TODO ensure transactional writes

    producer.setWriteTimestampToKafka(false) // TODO not sure if kafka timestamps will be of any use - deactivate for now
    producer
  }

  //  def createAvroKafkaProducer(topicId: String,
  //                             bootstrapServers: String,
  //                             partitioner: Option[FlinkKafkaPartitioner[Row]] = None)
  //                            (implicit env: StreamExecutionEnvironment): FlinkKafkaProducer[Row] = {
  //    val props = new Properties()
  //    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
  //
  //    // if no partitioner is specified, use the default Kafka partitioner (round-robin) instead of the FlinkFixedPartitioner
  //    val part: Optional[FlinkKafkaPartitioner[Row]] = Optional.ofNullable(partitioner.orNull)
  //
  //    val schema: Schema = SchemaBuilder.record("a").namespace("x")
  //      .fields()
  //      .name("f1").`type`("int").withDefault(1)
  //      .name("f2").`type`("int").withDefault(1)
  //      .endRecord()
  //
  //    val producer = new FlinkKafkaProducer[Row](topicId, // target topic
  //      new AvroRowSerializationSchema("") ,
  //      props, part)
  //
  //    // TODO ensure transactional writes
  //
  //    producer.setWriteTimestampToKafka(false) // TODO not sure if kafka timestamps will be of any use - deactivate for now
  //    producer
  //  }

  def createKafkaConsumer[T](topic: String, typeInfo: TypeInformation[T], props: Properties)(implicit env: StreamExecutionEnvironment): FlinkKafkaConsumer[T] = {
    val consumer = new FlinkKafkaConsumer[T](
      topic, new TypeInformationSerializationSchema[T](typeInfo, env.getConfig), props)
    consumer.setStartFromEarliest()
    consumer
  }

  def timeStampExtractor[T](maxOutOfOrderness: org.apache.flink.api.common.time.Time, extract: T => Long): AssignerWithPeriodicWatermarks[T] = {
    new BoundedOutOfOrdernessTimestampExtractor[T](org.apache.flink.streaming.api.windowing.time.Time.milliseconds(maxOutOfOrderness.toMilliseconds)) {
      override def extractTimestamp(element: T): Long = extract(element)
    }
  }

  def toSeq[T](listState: ListState[T]): Seq[T] =
    listState.get() match {
      case null => Nil // no state -> null!
      case xs@_ => xs.asScala.toSeq
    }
}
