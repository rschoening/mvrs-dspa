package utils

import java.net.URI

object TestUtils {
  def getResourceURIPath(resourcePath: String): String = new URI(s"file://${getClass.getResource(resourcePath).getPath}").toString
}
