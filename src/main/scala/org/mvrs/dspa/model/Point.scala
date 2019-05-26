package org.mvrs.dspa.model

import java.lang.Math.{pow, sqrt}

/**
  * Feature vector used for K-means clustering
  *
  */
final case class Point(features: Vector[Double]) {

  def distanceTo(that: Point): Double = sqrt(squaredDistanceTo(that))

  def squaredDistanceTo(that: Point): Double =
    features
      .view
      .zip(that.features)
      .map { case (x0, x1) => pow(x0 - x1, 2) }
      .sum

  def +(that: Point) = Point(
    features
      .zip(that.features)
      .map { case (x0, x1) => x0 + x1 })

  def -(that: Point) = Point(
    features
      .zip(that.features)
      .map { case (x0, x1) => x0 - x1 })

  def /(number: Int) = Point(features.map(_ / number))

  override def toString = s"Point(${features.mkString(", ")})"
}

object Point {
  def apply(values: Double*): Point = Point(Vector(values: _*)): Point
}