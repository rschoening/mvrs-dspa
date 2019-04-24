package org.mvrs.dspa

import com.typesafe.config.{Config, ConfigFactory}
import org.mvrs.dspa.io.ElasticSearchNode

import scala.collection.JavaConverters._

object Settings {
  val UnusualActivityControlFilePath = "c:\\temp\\activity-classification.txt"

  val config: Config = ConfigFactory.load()

  def elasticSearchNodes(): Seq[ElasticSearchNode] = {
    config.getObjectList("elasticsearch.hosts").asScala
      .map(_.toConfig)
      .map(cfg =>
        ElasticSearchNode(
          cfg.getString("name"),
          cfg.getInt("port"),
          cfg.getString("scheme")
        ))
  }
}
