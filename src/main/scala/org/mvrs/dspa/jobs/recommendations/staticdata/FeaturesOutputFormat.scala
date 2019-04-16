package org.mvrs.dspa.jobs.recommendations.staticdata

import org.mvrs.dspa.io.ElasticSearchUpsertOutputFormat

class FeaturesOutputFormat(uri: String, indexName: String, typeName: String)
  extends ElasticSearchUpsertOutputFormat[(Long, List[String])](uri, indexName, typeName) {

  override def getId(record: (Long, List[String])): String = record._1.toString

  override def getFields(record: (Long, List[String])): Iterable[(String, Any)] =
    Iterable(
      "features" -> record._2,
      "lastUpdate" -> System.currentTimeMillis
    )
}