package org.mvrs.dspa.utils.elastic

import org.apache.http.HttpHost

/**
  * Connection information for an ElasticSearch node
  *
  * @param hostname The hostname
  * @param port     The port
  * @param scheme   The URL scheme (default: http)
  */
case class ElasticSearchNode(hostname: String, port: Int = 9200, scheme: String = "http") {
  def httpHost: HttpHost = new HttpHost(hostname, port, scheme)

  val url: String = s"$scheme://$hostname:$port"

  override def toString: String = url
}
