package org.mvrs.dspa.utils

import java.util.concurrent.TimeUnit
import java.util.{Optional, Properties}

import org.apache.flink.api.common.serialization.{DeserializationSchema, SerializationSchema, TypeInformationSerializationSchema}
import org.apache.flink.api.common.state.ListState
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.configuration.{ConfigConstants, Configuration, WebOptions}
import org.apache.flink.streaming.api.datastream.{AsyncDataStream, DataStreamSink}
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.async.AsyncFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.Semantic
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper
import org.mvrs.dspa.utils.kafka.KafkaTopic

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

object FlinkUtils {
  /**
    * Convert from flink's common Time to the old streaming Time still used for windows
    *
    * @param time time instance to convert
    * @return streaming time instance
    */
  def convert(time: org.apache.flink.api.common.time.Time): org.apache.flink.streaming.api.windowing.time.Time =
    org.apache.flink.streaming.api.windowing.time.Time.milliseconds(time.toMilliseconds)

  def asyncStream[IN, OUT](dataStream: DataStream[IN], asyncFunction: AsyncFunction[IN, OUT])
                          (implicit timeout: Long = 5, timeUnit: TimeUnit = TimeUnit.SECONDS, capacity: Int = 5): DataStream[OUT] = {
    new DataStream[OUT](
      AsyncDataStream.unorderedWait(dataStream.javaStream, asyncFunction, timeout, timeUnit, capacity)
    )
  }

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
  def getTtl(time: Time, speedupFactor: Double = 0, minimumTimeMillis: Long = 60000): Time =
    Time.of(
      if (speedupFactor > 0)
        math.max(time.toMilliseconds / speedupFactor, minimumTimeMillis).toLong
      else
        minimumTimeMillis,
      TimeUnit.MILLISECONDS
    ) // TODO actual replay speed may be lower due to backpressure/slow source -> apply some factor to account for that? Or just let the caller reduce the speedup factor for this?

  def createStreamExecutionEnvironment(localWithUI: Boolean = false): StreamExecutionEnvironment = {
    if (localWithUI) {
      val config = new Configuration

      config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true)
      config.setString(WebOptions.LOG_PATH, "./data/flinkui.log")

      config.setString("metrics.reporters", "prometheus")
      config.setString("metrics.reporter.prometheus.class", "org.apache.flink.metrics.prometheus.PrometheusReporter")

      StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config)
    }
    else StreamExecutionEnvironment.getExecutionEnvironment
  }

  def createTypeInfoSerializationSchema[T: TypeInformation](implicit env: StreamExecutionEnvironment): TypeInformationSerializationSchema[T] =
    new TypeInformationSerializationSchema[T](createTypeInformation[T], env.getConfig)


  def createBatchExecutionEnvironment(localWithUI: Boolean = false): ExecutionEnvironment =
    if (localWithUI) ExecutionEnvironment.createLocalEnvironmentWithWebUI()
    else ExecutionEnvironment.getExecutionEnvironment

  def readCsv[T: ClassTag](path: String)
                          (implicit env: ExecutionEnvironment, typeInformation: TypeInformation[T]): DataSet[T] =
    env.readCsvFile[T](path, fieldDelimiter = "|", ignoreFirstLine = true)

  def writeToNewKafkaTopic[E](stream: DataStream[E],
                              topic: KafkaTopic[E],
                              numPartitions: Int = 5,
                              partitioner: Option[FlinkKafkaPartitioner[E]] = None,
                              replicationFactor: Short = 1,
                              semantic: FlinkKafkaProducer.Semantic = Semantic.AT_LEAST_ONCE): DataStreamSink[E] = {
    topic.create(numPartitions, replicationFactor, overwrite = true)
    stream.addSink(topic.producer(partitioner, semantic)).name(s"Kafka: ${topic.name}")
  }

  def createKafkaProducer[T](topicId: String,
                             bootstrapServers: String,
                             serializationSchema: SerializationSchema[T],
                             partitioner: Option[FlinkKafkaPartitioner[T]],
                             semantic: FlinkKafkaProducer.Semantic = Semantic.AT_LEAST_ONCE,
                             kafkaProducerPoolSize: Int = FlinkKafkaProducer.DEFAULT_KAFKA_PRODUCERS_POOL_SIZE,
                             writeTimestampToKafka: Boolean = false): FlinkKafkaProducer[T] = {
    // if no partitioner is specified, use the default Kafka partitioner (round-robin) instead of the FlinkFixedPartitioner
    val customPartitioner: Optional[FlinkKafkaPartitioner[T]] = Optional.ofNullable(partitioner.orNull)

    val producer = new FlinkKafkaProducer[T](
      topicId,
      new KeyedSerializationSchemaWrapper(serializationSchema),
      kafka.connectionProperties(bootstrapServers),
      customPartitioner,
      semantic,
      kafkaProducerPoolSize
    )

    // NOTE: EXACTLY_ONCE requires a larger-than-default value for transaction.max.timeout.ms (--> 1h, 3600000)
    producer.setWriteTimestampToKafka(writeTimestampToKafka)

    producer
  }

  def createKafkaConsumer[T](topic: String,
                             props: Properties,
                             deserializationSchema: DeserializationSchema[T],
                             commitOffsetsOnCheckpoints: Boolean): FlinkKafkaConsumer[T] = {
    val consumer = new FlinkKafkaConsumer[T](topic, deserializationSchema, props)
    consumer.setStartFromEarliest() // by default, start from earliest. Caller can override on returned instance
    consumer.setCommitOffsetsOnCheckpoints(commitOffsetsOnCheckpoints)
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

  /**
    * Generates random delay values, with a given mean and standard deviation, and not exceeding a given maximum value.
    * Default values are chosen for a not too unrealistic skewed delay distribution (maximum value = mean + 1.5 stdev)
    *
    * @param rand the random number generator
    * @param maximumDelayMilliseconds the maximum delay value.
    * @param mean the mean delay value
    * @param standardDeviation the standard deviation
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
}
