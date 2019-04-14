package org.mvrs.dspa

import java.nio.file.Paths
import java.time.{Instant, LocalDateTime, ZoneId, ZonedDateTime}
import java.util.{Base64, Optional, Properties}

import com.sksamuel.elastic4s.http.index.admin.DeleteIndexResponse
import com.sksamuel.elastic4s.http.{ElasticClient, Response}
import com.twitter.algebird.{MinHashSignature, MinHasher, MinHasher32}
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time
import org.mvrs.dspa.io.ElasticSearchOutputFormat

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

// NOTE this will be refactored, currently a mixed bag
package object utils {

  val kafkaBrokers = "localhost:9092"
  private val dateFormat = ThreadLocal.withInitial[SimpleDateFormat](() => new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))

  def toDateTime(millis: Long): LocalDateTime = LocalDateTime.from(Instant.ofEpochMilli(millis))

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

  def getMinHashSignature(features: Seq[String], minHasher: MinHasher32): MinHashSignature =
    minHasher.combineAll(features.map(minHasher.init))

  def decodeMinHashSignature(base64: String) = MinHashSignature(Base64.getDecoder.decode(base64))

  def createMinHasher(numHashes: Int = 100, targetThreshold: Double = 0.2): MinHasher32 =
    new MinHasher32(numHashes, MinHasher.pickBands(targetThreshold, numHashes))

  def dropIndex(client: ElasticClient, indexName: String): Response[DeleteIndexResponse] = {
    // we must import the dsl
    import com.sksamuel.elastic4s.http.ElasticDsl._

    client.execute {
      deleteIndex(indexName)
    }.await
  }

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

  def timeStampExtractor[T](maxOutOfOrderness: Time, extract: T => Long): AssignerWithPeriodicWatermarks[T] = {
    new BoundedOutOfOrdernessTimestampExtractor[T](maxOutOfOrderness) {
      override def extractTimestamp(element: T): Long = extract(element)
    }
  }

  class BucketsOutputFormat(uri: String, indexName: String, typeName: String)
    extends ElasticSearchOutputFormat[(Long, Seq[(Long, MinHashSignature)])](uri) {

    import com.sksamuel.elastic4s.http.ElasticDsl._

    override def process(record: (Long, Seq[(Long, MinHashSignature)]), client: ElasticClient): Unit = {
      // NOTE: connections are "unexpectedly closed" when using onComplete on the future - need to await
      client.execute {
        indexInto(indexName / typeName)
          .withId(record._1.toString)
          .fields(
            "users" -> record._2.map(t => Map(
              "uid" -> t._1,
              "minhash" -> Base64.getEncoder.encodeToString(t._2.bytes))),
            "lastUpdate" -> System.currentTimeMillis())
      }.await // synchronous per-record round trip --> slow but sufficient for loading static data
    }
  }

}
