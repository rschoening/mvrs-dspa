package org.mvrs.dspa.streams

import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.get.GetResponse
import com.sksamuel.elastic4s.http.{ElasticClient, Response}
import org.apache.flink.api.scala._
import org.mvrs.dspa.model.{CommentEvent, RawCommentEvent}
import org.mvrs.dspa.streams.AsyncPostMappingLookupFunction._
import org.mvrs.dspa.utils.elastic.{AsyncCachingElasticSearchFunction, ElasticSearchNode}

import scala.concurrent.Future

/**
  * Async IO function to look up known persons from an ElasticSearch index, and removing them from the set of candidates.
  *
  * @param postMappingIndex The name of the ElasticSearch index storing the persons known to a person
  * @param postMappingType  The type for storing the persons known to a person.
  * @param nodes            the elastic search nodes to connect to
  */
class AsyncPostMappingLookupFunction(postMappingIndex: String, postMappingType: String, nodes: ElasticSearchNode*)
  extends AsyncCachingElasticSearchFunction[RawCommentEvent, Either[RawCommentEvent, CommentEvent], Long, GetResponse](
    getCacheKey, nodes, cacheEmptyResponse = false) {

  /**
    * Derives the value to cache based on the input element and retrieved output element, in case of a cache miss.
    *
    * @param input  the input element
    * @param output the output element
    * @return the value to cache, which must be serializable
    */
  override protected def getCacheValue(input: RawCommentEvent, output: Either[RawCommentEvent, CommentEvent]): Option[Long] =
    output match {
      case Left(_) => None
      case Right(commentEvent) => Some(commentEvent.postId)
    }

  /**
    * If the comment is a reply, return None (cache hit or query required)
    *
    * @param input the input element
    * @return the output element to emit if direct conversion is possible, otherwise None
    */
  override protected def toOutput(input: RawCommentEvent): Option[Either[RawCommentEvent, CommentEvent]] =
    input.replyToPostId match {
      case Some(postId) => Some(Right(CommentEvent(input, postId)))
      case None => None
    }

  /**
    * Derives the output element based on the input element and the corresponding cached value, in case of a cache hit.
    *
    * @param input       the input element
    * @param cachedValue the cached value
    * @return the output element to emit
    */
  override protected def toOutput(input: RawCommentEvent, cachedValue: Long): Either[RawCommentEvent, CommentEvent] =
    Right(CommentEvent(input, cachedValue))

  /**
    * Initiates the query to ElasticSearch and returns the future response
    *
    * @param client the ElasticSearch client
    * @param input  the input element
    * @return the future response
    */
  override protected def executeQuery(client: ElasticClient, input: RawCommentEvent): Future[Response[GetResponse]] =
    client.execute {
      get(input.replyToCommentId.get.toString) from postMappingIndex / postMappingType
    }

  /**
    * Unpacks the output element from the response from ElasticSearch
    *
    * @param response the response from ElasticSearch
    * @param input    the input element
    * @return the output element, or None for empty response
    */
  override protected def unpackResponse(response: Response[GetResponse],
                                        input: RawCommentEvent): Option[Either[RawCommentEvent, CommentEvent]] =
    response.result match {
      case r if !r.found => Some(Left(input))
      case r@_ =>
        Some(
          Right(
            CommentEvent(
              input,
              r.sourceAsMap("postId").asInstanceOf[Integer].toLong // casting to Long directly fails
            )
          )
        )
    }
}

/**
  * Companion object
  */
object AsyncPostMappingLookupFunction {
  /**
    * Get the cache key for the raw comment
    *
    * @param input The raw comment event
    * @return parent comment id for replies, comment id of first-level comments
    */
  def getCacheKey(input: RawCommentEvent): String =
    (input.replyToCommentId match {
      case Some(parentCommentId) => parentCommentId
      case None => input.commentId
    }).toString
}


