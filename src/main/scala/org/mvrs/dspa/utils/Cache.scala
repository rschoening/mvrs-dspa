package org.mvrs.dspa.utils

import java.time.Instant

import com.github.benmanes.caffeine.cache.Caffeine
import io.chrisdavenport.read.Read
import javax.annotation.Nonnegative
import scalacache.caffeine.CaffeineCache
import scalacache.modes.sync._
import scalacache.{Entry, sync}

import scala.collection.JavaConverters._

/**
  * Cache with bounded size and LRU-based and ttl-based eviction, based on
  * scalacache ([[https://github.com/cb372/scalacache]]) backed by Caffeine [[https://github.com/ben-manes/caffeine]]
  * and using the Read Typeclass from [[https://github.com/ChristopherDavenport/read]] for converting from the internal
  * string keys to the specified key type.
  *
  * @param maximumSize the maximum cache size (not a hard limit, see 'maximumSize' in
  *                    [[https://github.com/ben-manes/caffeine/blob/master/caffeine/src/main/java/com/github/benmanes/caffeine/cache/Caffeine.java]])
  * @param read        implicit reader to convert the cache key from String to the key type
  * @tparam K key type
  * @tparam V value type
  */
class Cache[K: Read, V](@Nonnegative maximumSize: Long = 10000L)
                       (implicit read: Read[K]) {
  private val underlyingCaffeineCache =
    Caffeine
      .newBuilder()
      .maximumSize(maximumSize)
      .build[String, Entry[V]]

  private implicit val scalaCache: scalacache.Cache[V] = CaffeineCache(underlyingCaffeineCache)

  def get(key: K): Option[V] = sync.get(key)

  def put(key: K, value: V, ttl: Option[scala.concurrent.duration.Duration] = None): Unit = sync.put(key)(value, ttl)

  def clear(): Unit = scalaCache.removeAll()

  def toMap: Map[K, V] = underlyingCaffeineCache.asMap().asScala.map(e => (read.unsafeRead(e._1), e._2.value)).toMap

  def putAll(pairs: Iterable[(K, V)]): Unit =
    underlyingCaffeineCache.putAll(
      pairs.map { case (key, value) => (key.toString, Entry(value, None)) }
        .toMap
        .asJava
    )

  def putAllWithExpiration(pairs: Iterable[(K, V, Option[Instant])]): Unit =
    underlyingCaffeineCache.putAll(
      pairs.map { case (key, value, expiresAt) => (key.toString, Entry(value, expiresAt)) }
        .toMap
        .asJava
    )

  def estimatedSize: Long = underlyingCaffeineCache.estimatedSize()
}
