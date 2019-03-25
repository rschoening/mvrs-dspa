package org.mvrs.dspa.trials

import com.sksamuel.elastic4s.RefreshPolicy
import com.sksamuel.elastic4s.http.search.SearchResponse
import com.sksamuel.elastic4s.http.{ElasticClient, ElasticProperties, RequestFailure, RequestSuccess}

object ElasticTrial extends App {
  val client = ElasticClient(ElasticProperties("http://localhost:9200"))

  // we must import the dsl
  import com.sksamuel.elastic4s.http.ElasticDsl._

  // Next we create an index in advance ready to receive documents.
  // await is a helper method to make this operation synchronous instead of async
  // You would normally avoid doing this in a real program as it will block
  // the calling thread but is useful when testing

  // NOTE: apparently noop if index already exists
  client.execute {
    createIndex("artists").mappings(
      mapping("modern").fields(
        textField("name")
      )
    )
  }.await

  // Next we index a single document which is just the name of an Artist.
  // The RefreshPolicy.Immediate means that we want this document to flush to the disk immediately.
  // see the section on Eventual Consistency.
  client.execute {
    indexInto("artists" / "modern")
      .withId("some_id")
      .fields("name" -> "L.S. Lowry WITH ID")
      .refresh(RefreshPolicy.Immediate)
  }

  // upsert by id
  client.execute {
    update("some_id").in("artists/modern").docAsUpsert(
      "name" -> "Lowry 2"
    )
    update("another_id").in("artists/modern").docAsUpsert(
      "name" -> "XYz1"
    )
  }.await


  // now we can search for the documents we just indexed
  val resp = client.execute {
    search("artists/modern").query(idsQuery(List("some_id", "another_id")))
  }.await

  // resp is a Response[+U] ADT consisting of either a RequestFailure containing the
  // Elasticsearch error details, or a RequestSuccess[U] that depends on the type of request.
  // In this case it is a RequestSuccess[SearchResponse]

  println("---- Search Results ----")
  resp match {
    case failure: RequestFailure => println("We failed " + failure.error)
    case results: RequestSuccess[SearchResponse] => println(results.result.hits.hits.toList.mkString("\n"))

    // case results: RequestSuccess[_] => println(results.result)
  }

  // Response also supports familiar combinators like map / flatMap / foreach:
  resp foreach (search => println(s"There were ${search.totalHits} total hits"))


  client.execute {
    catIndices()
  }.await.result.foreach(println)

  client.execute {
    getMapping("features" / "personFeatures")
  }.await.result.foreach(_.mappings.foreach(println))

  client.execute {
    search("features" / "personFeatures") // 10 hits returned (we'll anyway always do point queries)
  }.await.foreach(resp => resp.hits.hits.foreach(h => println(s"$h")))

  client.execute {
    get("635").from("features" / "personFeatures")
  }.await.foreach(resp => println(resp))

  client.close()
}
