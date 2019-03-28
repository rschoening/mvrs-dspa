package org.mvrs.dspa.recommendations.staticData

import org.mvrs.dspa.io.ElasticSearchUpsertOutputFormat

class KnownUsersOutputFormat(uri: String, indexName: String, typeName: String)
  extends ElasticSearchUpsertOutputFormat[(Long, List[Long])](uri, indexName, typeName) {

  override def getId(record: (Long, List[Long])): String = record._1.toString

  override def getFields(record: (Long, List[Long])): Iterable[(String, Any)] =
    Iterable(
      "knownUsers" -> record._2,
      "lastUpdate" -> System.currentTimeMillis
    )
}
