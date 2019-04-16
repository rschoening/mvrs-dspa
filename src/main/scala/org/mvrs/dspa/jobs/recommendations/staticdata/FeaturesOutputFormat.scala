package org.mvrs.dspa.jobs.recommendations.staticdata

import org.mvrs.dspa.io.{ElasticSearchNode, ElasticSearchUpsertOutputFormat}

class FeaturesOutputFormat(indexName: String, typeName: String, nodes: ElasticSearchNode*)
  extends ElasticSearchUpsertOutputFormat[(Long, List[String])](indexName, typeName, nodes: _*) {

  override def getId(record: (Long, List[String])): String = record._1.toString

  override def getFields(record: (Long, List[String])): Iterable[(String, Any)] =
    Iterable(
      "features" -> record._2,
      "lastUpdate" -> System.currentTimeMillis
    )
}