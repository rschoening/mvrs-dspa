package org.mvrs.dspa

import com.typesafe.config.{Config, ConfigFactory}
import org.mvrs.dspa.elastic.ElasticSearchNode

import scala.collection.JavaConverters._

object Settings {
  /**
    *  The configuration read from application.conf (all resources with this name on classpath)
    *
    *  see https://github.com/lightbend/config#overview
    */
  val config: Config = ConfigFactory.load()

  val elasticSearchNodes: Seq[ElasticSearchNode] =
    config.getObjectList("elasticsearch.hosts").asScala
      .map(_.toConfig)
      .map(cfg =>
        ElasticSearchNode(
          cfg.getString("name"),
          cfg.getInt("port"),
          cfg.getString("scheme")
        ))
}
