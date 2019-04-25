package org.mvrs.dspa.elastic

import org.apache.http.HttpHost

case class ElasticSearchNode(hostname: String, port: Int = 9200, scheme: String = "http") {
  def httpHost: HttpHost = new HttpHost(hostname, port, scheme)

  val url: String = s"$scheme://$hostname:$port"

  override def toString: String = url
}
