package org.mvrs.dspa.jobs.recommendations.staticdata

import org.mvrs.dspa.io.{ElasticSearchNode, ElasticSearchUpsertOutputFormat}

class BucketsOutputFormat(indexName: String, typeName: String, nodes: ElasticSearchNode*)
  extends ElasticSearchUpsertOutputFormat[(Long, List[Long])](indexName, typeName, nodes: _*) {

  override def getId(record: (Long, List[Long])): String = record._1.toString

  override def getFields(record: (Long, List[Long])): Iterable[(String, Any)] =
    Iterable(
      "uid" -> record._2,
      "lastUpdate" -> System.currentTimeMillis()
    )
}