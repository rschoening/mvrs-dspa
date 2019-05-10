package org.mvrs.dspa.utils

import com.github.benmanes.caffeine.cache.Caffeine
import io.chrisdavenport.read.Read
import javax.annotation.Nonnegative
import scalacache.caffeine.CaffeineCache
import scalacache.modes.sync._
import scalacache.{Entry, sync}

import scala.collection.JavaConverters._

class Cache[K: Read, V](@Nonnegative maximumSize: Long = 10000L)(implicit read: Read[K]) {
  private val underlyingCaffeineCache = Caffeine.newBuilder().maximumSize(maximumSize).build[String, Entry[V]]
  implicit val scalaCache: scalacache.Cache[V] = CaffeineCache(underlyingCaffeineCache)

  def get(key: K): Option[V] = sync.get(key)

  def put(key: K, value: V, ttl: Option[scala.concurrent.duration.Duration] = None): Unit = sync.put(key)(value, ttl)

  def clear(): Unit = scalaCache.removeAll()

  def toMap: Map[K, V] = underlyingCaffeineCache.asMap().asScala.map(e => (read.unsafeRead(e._1), e._2.value)).toMap

  def putAll(pairs: Map[K, V]): Unit = underlyingCaffeineCache.putAll(
    pairs.map {
      case (key, value) => (key.toString, Entry(value, None))
    }.asJava)
}
