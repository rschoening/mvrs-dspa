package org.mvrs.dspa.clustering

import java.util

import com.sksamuel.elastic4s.http.ElasticClient
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest

import scala.collection.JavaConverters._

object ActivityClassificationIndex {
  def create(client: ElasticClient, indexName: String, typeName: String): Unit = {
    import com.sksamuel.elastic4s.http.ElasticDsl._

    // NOTE: apparently noop if index already exists
    client.execute {
      createIndex(indexName).mappings(
        mapping(typeName).fields(
          longField("personId"),
          longField(name = "eventId"),
          intField(name = "clusterIndex"),
          doubleField(name = "clusterWeight"),
          doubleField(name = "clusterCentroid").index(false),
          dateField("timestamp")
        )
      )
    }.await
  }

  private def createDocument(record: ClassifiedEvent): util.Map[String, Any] = {
    Map[String, Any](
      "personId" -> record.personId,
      "eventId" -> record.eventId,
      "clusterIndex" -> record.cluster.index,
      "clusterWeight" -> record.cluster.weight,
      "clusterCentroid" -> record.cluster.centroid.features.asJava, // Note: conversion to Java collection required
      "timestamp" -> record.timestamp
    ).asJava
  }

  def createSink(hostname: String, port: Int, scheme: String,
                 indexName: String, typeName: String): ElasticsearchSink[ClassifiedEvent] = {
    // TODO directly pass in list of httphosts
    val hosts = List(new HttpHost(hostname, port, scheme))

    val esSinkBuilder = new ElasticsearchSink.Builder[ClassifiedEvent](
      hosts.asJava,
      new ElasticsearchSinkFunction[ClassifiedEvent] {
        def createIndexRequest(record: ClassifiedEvent): IndexRequest = {

          new IndexRequest(indexName, typeName).source(createDocument(record))
        }

        override def process(record: ClassifiedEvent, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          requestIndexer.add(createIndexRequest(record))
        }
      }
    )

    // buffering has a huge influence on performance
    // TODO test behavior with persistence errors
    esSinkBuilder.setBulkFlushMaxActions(200)

    //    // provide a RestClientFactory for custom configuration on the internally created REST client
    //    //    esSinkBuilder.setRestClientFactory(
    //    //      restClientBuilder => {
    //    //         restClientBuilder.setDefaultHeaders(...)
    //    //         restClientBuilder.setMaxRetryTimeoutMillis(...)
    //    //         restClientBuilder.setPathPrefix(...)
    //    //         restClientBuilder.setHttpClientConfigCallback(...)
    //    //      }
    //    //    )
    //
    esSinkBuilder.build()
  }
}
