package org.mvrs.dspa.model

/**
  * A recommendation of similar persons for a given person
  *
  * @param personId        The id of the person that receives the recommendation
  * @param recommendations The list of recommended persons with their similarity
  */
case class Recommendation(personId: Long, recommendations: Seq[RecommendedPerson])

/**
  * A recommended person
  *
  * @param personId   The id of the recommended person
  * @param similarity The approximated Jaccard similarity between the recommended person and the target person
  */
case class RecommendedPerson(personId: Long, similarity: Double)
