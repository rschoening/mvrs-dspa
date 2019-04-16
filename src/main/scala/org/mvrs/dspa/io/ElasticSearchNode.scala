package org.mvrs.dspa.io

import org.apache.http.HttpHost

case class ElasticSearchNode(hostname: String, port: Int = 9200, scheme: String = "http") {
  def httpHost: HttpHost = new HttpHost(hostname, port, scheme)
}
