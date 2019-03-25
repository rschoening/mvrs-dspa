package org.mvrs.dspa.trials

import java.nio.file.Paths

import com.sksamuel.elastic4s.http.{ElasticClient, ElasticProperties}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.IngestionTimeExtractor
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.mvrs.dspa.recommendations.Recommendations.GroupInterestsProcessFunction


object LoadRecommendationFeatures extends App {
  val client = ElasticClient(ElasticProperties("http://localhost:9200"))

  recreateRecommendationsIndex(client)


  val env = StreamExecutionEnvironment.getExecutionEnvironment

  env.setParallelism(1)
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  val rootPath = raw"C:\\data\\dspa\\project\\1k-users-sorted\\tables"
  val hasInterestCsv = Paths.get(rootPath, "person_hasInterest_tag.csv").toString
  val worksAtCsv = Paths.get(rootPath, "person_workAt_organisation.csv").toString

  val workStream = env
    .readTextFile(worksAtCsv)
    .filter(str => !str.startsWith("Person.id|") && str != "")
    .map(parseInterest(_, "W"))

  val tagsStream = env
    .readTextFile(hasInterestCsv)
    .filter(str => !str.startsWith("Person.id|") && str != "")
    .map(parseInterest(_, "T"))

  // union with dummy stream that does not end - otherwise the last trigger, after all sources close, is not processed!
  // workaround: https://stackoverflow.com/questions/52467413/processing-time-windows-doesnt-work-on-finite-data-sources-in-apache-flink
  val dummyInterest: String = ""
  val dummyStream: DataStream[(Long, String)] = env.addSource((context: SourceContext[(Long, String)]) => {
    while (true) {
      Thread.sleep(1000)
      context.collect((-1, dummyInterest))
    }
  })

  // use ingestion time as event time, so we can apply time window with the time semantics we need later (when processing event streams)
  // note that times from this stream will not be compared with times from the event stream (resulting bucket state will be broadcast to event stream)
  val interestsStream: KeyedStream[(Long, String), Long] =
  tagsStream
    .union(
      tagsStream,
      dummyStream // the dummy stream to ensure trigger after static sources close
    ) // TODO combine with all other streams (study, tag classes etc.)
    .filter(_._2 != dummyInterest) // drop the dummy events
    .assignTimestampsAndWatermarks(new IngestionTimeExtractor[(Long, String)]())
    .keyBy(_._1) // (*uid*, interest)


  // collect the users interests in a set
  val groupedInterestsStream: KeyedStream[(Long, Set[String]), Long] =
    interestsStream
      .process(new GroupInterestsProcessFunction(Time.seconds(5).toMilliseconds)) // (uid, Set[interest])
      .keyBy(_._1) // (*uid*, Set[interest])

  groupedInterestsStream.addSink(new InterestsSinkFunction)
//  groupedInterestsStream.print

  env.execute("Import recommendations")

  // we must import the dsl
  import com.sksamuel.elastic4s.http.ElasticDsl._


  client.execute {
    catIndices()
  }.await.result.foreach(println)

  client.execute {
    getMapping("recommendation/personFeatures")
  }.await.result.foreach(_.mappings.foreach(println))

  client.execute {
    update("1").in("recommendation/personFeatures").docAsUpsert(
      "features" -> Set("a", "b", "c")
    )
  }.await

  client.execute {
    get("1").from("recommendation/personFeatures")
  }.await.foreach(println)

  client.close()

  private def parseInterest(str: String, prefix: String): (Long, String) = {
    val tokens = str.split('|')
    (tokens(0).trim.toLong, prefix + tokens(1).trim)
  }

  private def recreateRecommendationsIndex(client: ElasticClient): Unit = {
    // we must import the dsl
    import com.sksamuel.elastic4s.http.ElasticDsl._

    // Next we create an index in advance ready to receive documents.
    // await is a helper method to make this operation synchronous instead of async
    // You would normally avoid doing this in a real program as it will block
    // the calling thread but is useful when testing

    client.execute {
      deleteIndex("recommendation")
    }.await

    // NOTE: apparently noop if index already exists
    client.execute {
      createIndex("recommendation").mappings(
        mapping("personFeatures").fields(
          textField("features"),
          dateField(name = "lastUpdate")
        )
      )
    }.await

  }

  class InterestsSinkFunction extends RichSinkFunction[(Long, Set[String])] {
    // we must import the dsl
    import com.sksamuel.elastic4s.http.ElasticDsl._

    var client: ElasticClient = _

    override def invoke(value: (Long, Set[String]), context: SinkFunction.Context[_]): Unit = {

      client.execute {
        indexInto("recommendation" / "personFeatures")
          .withId(value._1.toString)
          .fields(
            "features" -> value._2,
            "lastUpdate" -> System.currentTimeMillis())
      }.await

      println(s"written interests for user ${value._1}")
    }

    override def open(parameters: Configuration): Unit = {
      // TODO get address and other parameters from config
      client = ElasticClient(ElasticProperties("http://localhost:9200"))
    }
  }

}
