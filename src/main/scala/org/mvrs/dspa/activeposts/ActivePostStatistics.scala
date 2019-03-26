package org.mvrs.dspa.activeposts

import java.util.Properties

import com.sksamuel.elastic4s.http.{ElasticClient, ElasticProperties}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.time.Time
import org.mvrs.dspa.activeposts.EventType.EventType
import org.mvrs.dspa.events.{CommentEvent, LikeEvent, PostEvent, PostStatistics}
import org.mvrs.dspa.functions.ScaledReplayFunction
import org.mvrs.dspa.utils

object ActivePostStatistics extends App {

  val elasticSearchUri = "http://localhost:9200"
  val indexName = "statistics"
  val typeName = "postStatistics"

  val client = ElasticClient(ElasticProperties(elasticSearchUri))
  try {
    utils.dropIndex(client, indexName)
    createStatisticsIndex(client, indexName, typeName)
  }
  finally {
    client.close()
  }


  val props = new Properties()
  props.setProperty("bootstrap.servers", "localhost:9092")
  props.setProperty("group.id", "test")
  props.setProperty("isolation.level", "read_committed")

  implicit val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  env.setParallelism(3)

  val commentsSource = utils.createKafkaConsumer("comments", createTypeInformation[CommentEvent], props)
  val postsSource = utils.createKafkaConsumer("posts", createTypeInformation[PostEvent], props)
  val likesSource = utils.createKafkaConsumer("likes", createTypeInformation[LikeEvent], props)

  val speedupFactor = 0; // 10000000000L
  val randomDelay = 0 // TODO input or scaled time? --> probably input time
  val maxOutOfOrderness = Time.milliseconds(randomDelay)

  val commentsStream: DataStream[CommentEvent] = env
    .addSource(commentsSource)
    .process(new ScaledReplayFunction[CommentEvent](_.timeStamp, speedupFactor, randomDelay))

  val postsStream: DataStream[PostEvent] = env
    .addSource(postsSource)
    .process(new ScaledReplayFunction[PostEvent](_.timeStamp, speedupFactor, randomDelay))

  val likesStream: DataStream[LikeEvent] = env
    .addSource(likesSource)
    .process(new ScaledReplayFunction[LikeEvent](_.timeStamp, speedupFactor, randomDelay))

  val stats = statistics(
    commentsStream, postsStream, likesStream,
    Time.hours(12).toMilliseconds,
    Time.minutes(30).toMilliseconds)

  // stats.print
  stats.addSink(new StatisticsSinkFunction(elasticSearchUri, indexName, typeName))

  env.execute("post statistics")

  //noinspection ConvertibleToMethodValue
  def statistics(commentsStream: DataStream[CommentEvent],
                 postsStream: DataStream[PostEvent],
                 likesStream: DataStream[LikeEvent],
                 windowSize: Long,
                 slide: Long): DataStream[PostStatistics] = {
    val comments: KeyedStream[Event, Long] = commentsStream
      .assignTimestampsAndWatermarks(utils.timeStampExtractor[CommentEvent](maxOutOfOrderness, _.timeStamp))
      .map(createEvent(_))
      .keyBy(_.postId)

    val posts: KeyedStream[Event, Long] = postsStream
      .assignTimestampsAndWatermarks(utils.timeStampExtractor[PostEvent](maxOutOfOrderness, _.timeStamp))
      .map(createEvent(_))
      .keyBy(_.postId)

    val likes: KeyedStream[Event, Long] = likesStream
      .assignTimestampsAndWatermarks(utils.timeStampExtractor[LikeEvent](maxOutOfOrderness, _.timeStamp))
      .map(createEvent(_))
      .keyBy(_.postId)

    posts
      .union(comments, likes)
      .keyBy(_.postId)
      .process(new PostStatisticsFunction(windowSize, slide))
  }

  private def createEvent(e: LikeEvent) = Event(EventType.Like, e.postId, e.personId, e.timeStamp)


  private def createEvent(e: PostEvent) = Event(EventType.Post, e.id, e.personId, e.timeStamp)


  private def createEvent(e: CommentEvent) = Event(
    e.replyToCommentId.map(_ => EventType.Reply).getOrElse(EventType.Comment),
    e.postId, e.personId, e.timeStamp)


  private def createStatisticsIndex(client: ElasticClient, indexName: String, typeName: String): Unit = {
    import com.sksamuel.elastic4s.http.ElasticDsl._

    // NOTE: apparently noop if index already exists
    client.execute {
      createIndex(indexName).mappings(
        mapping(typeName).fields(
          longField("postId"),
          intField("replyCount"),
          intField("likeCount"),
          intField("commentCount"),
          intField("distinctUserCount"),
          dateField("timestamp")
        )
      )
    }.await

  }

}

object EventType extends Enumeration {
  type EventType = Value
  val Post, Comment, Reply, Like = Value
}

case class Event(eventType: EventType, postId: Long, personId: Long, timestamp: Long)

class StatisticsSinkFunction(uri: String, indexName: String, typeName: String) extends RichSinkFunction[PostStatistics] {
  private var client: Option[ElasticClient] = None

  import com.sksamuel.elastic4s.http.ElasticDsl._

  override def open(parameters: Configuration): Unit = client = Some(ElasticClient(ElasticProperties(uri)))

  override def close(): Unit = {
    client.foreach(_.close())
    client = None
  }

  override def invoke(value: PostStatistics, context: SinkFunction.Context[_]): Unit = process(value, client.get, context)


  private def process(record: PostStatistics, client: ElasticClient, context: SinkFunction.Context[_]): Unit = {
    client.execute {
      indexInto(indexName / typeName)
        .fields("postId" -> record.postId,
          "replyCount" -> record.replyCount,
          "commentCount" -> record.commentCount,
          "likeCount" -> record.likeCount,
          "distinctUserCount" -> record.distinctUsersCount,
          "timestamp" -> record.time)
    }.await
  }
}
