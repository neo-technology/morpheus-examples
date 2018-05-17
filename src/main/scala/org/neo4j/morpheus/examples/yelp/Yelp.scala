package org.neo4j.morpheus.examples.yelp
import java.io.File
import java.util.UUID

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.neo4j.cypher.spark.EnterpriseNeo4jGraphSource
import org.neo4j.morpheus.examples.accidents.Graphable
import org.opencypher.okapi.api.io.conversion.NodeMapping
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.CAPSNodeTable
import java.net.URI

import org.opencypher.okapi.api.graph.GraphName

/*
 * To use this example you must download the Yelp dataset challenge (JSON format)
 * found here: https://www.yelp.co.uk/dataset/download
 *
 * Additionally, generate one sample file:
 * head -n 1000 review.json > review-samp.json
 *
 * Which allows running on a smaller volume of test data.
 *
 * It is roughly 6gb uncompressed.
 */
class YelpData(jsonFile:String, attrs:Seq[String])(implicit spark:SparkSession, session:CAPSSession) extends Graphable {
  val dfTmp = capsWorkaround(spark.read.json(jsonFile)
      .withColumn(reservedId, curId))
      .select(reservedId, attrs:_*)

  // properties like.this mess up CAPS, so rename them
  val df = dfTmp.toDF(dfTmp.columns.map(_.replaceAll("\\.", "_")): _*)

  // println("Top 5 of " + jsonFile)
  // df.show(5)

  def asGraph(label: String) = {
    // When you select foo.bar out of JSON, it shows up as just "bar"
    // So the mapping needs to be aware of the suffix part, and not
    // the full select attr name.
    val mapping = NodeMapping.withSourceIdKey(reservedId)
      .withImpliedLabel(label)
      .withPropertyKeys(attrs.map(_.replaceAll(".*\\.", "")):_*)

    session.readFrom(CAPSNodeTable(mapping, df))
  }
}

object Yelp extends App {
  override def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)

    if (!new File(args(0)).isDirectory) {
      println("Call me with a directory path to the Yelp challenge dataset, JSON version")
      return None
    }

    val dir = args(0)

    implicit val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName(s"caps-local-${UUID.randomUUID()}")
      .getOrCreate()

    // 1) Create CAPS session and retrieve Spark session
    implicit val session: CAPSSession = CAPSSession.create(spark)

    YelpFiles.files.map(set => {
      val yelpGraph = new YelpData(dir + File.separator + set._1, set._3).asGraph(set._2)
      session.catalog.store(set._2, yelpGraph)
    })

    // Store the integrated graph in the catalog.
    session.catalog.store("yelp", session.cypher(
      """
        |FROM GRAPH Business
        |   MATCH (b:Business)
        |FROM GRAPH Review
        |   MATCH (r:Review)
        |FROM GRAPH Tip
        |   MATCH (t:Tip)
        |FROM GRAPH User
        |   MATCH (u:User)
        |
        |WHERE
        |   r.business_id = b.business_id AND
        |   t.business_id = b.business_id AND
        |   t.user_id = u.user_id AND
        |   r.user_id = u.user_id
        |
        |CONSTRUCT ON Business, Review, Tip, User
        |
        |CLONE b, r, t, u
        | NEW (b)-[:REVIEW]->(r)
        | NEW (b)-[:TIP]->(t)<-[:POSTED]-(u)
        | NEW (u)-[:REVIEWED]->(b)
        | NEW (u)-[:POSTED]->(r)
        |
        |RETURN GRAPH
      """.stripMargin).getGraph)


      /*
       * This code fails due to a bug in string escaping.
       * Carded here: https://trello.com/c/EXDq6dKY/230-enterpriseneo4jpropertygraphsource-does-not-properly-escape-strings-when-writing-to-neo4j-via-driver
      val neo4j = EnterpriseNeo4jGraphSource(new URI("bolt://neo4j:admin@localhost"))
      println("Storing yelp graph")
      neo4j.store(GraphName("yelp"), session.catalog.graph("yelp"))
      */

      // Show some really mean reviews.  :)  Namely very negative
      // reviews of businesses that are overall favorable.
      session.catalog.graph("yelp").cypher(
        """
          |MATCH (b:Business)-[:REVIEW]->(r:Review)<-[:POSTED]-(u:User)
          |WHERE r.stars < 3
          |RETURN u.user_id, u.name, u.average_stars, b.name, b.stars, r.stars, r.text
          |LIMIT 50
        """.stripMargin).getRecords.show

  }
}

object YelpFiles {
  val files = Seq(
    // Format:  filename, label, Seq(attributes)
    ("business.json", "Business", Seq(
      "address", "attributes.AcceptsInsurance", "attributes.BusinessAcceptsCreditCards",
      "attributes.ByAppointmentOnly", "business_id",
      "city", "latitude", "longitude", "name", "postal_code", "review_count",
      "stars", "state"
    )),
    // Change to "review.json" for full dataset (large)
    ("review-samp.json", "Review", Seq(
      "business_id", "cool", "date", "funny", "review_id",
      "stars", "text", "useful", "user_id"
    )),
    ("tip.json", "Tip", Seq(
      "business_id", "date", "likes", "text", "user_id"
    )),
    ("user.json", "User", Seq(
      "average_stars", "compliment_cool", "compliment_cute",
      "compliment_funny", "compliment_hot", "compliment_list",
      "compliment_more", "compliment_note", "compliment_photos",
      "compliment_plain", "compliment_profile", "compliment_writer",
      "cool", "fans", "funny", "name", "review_count", "useful",
      "user_id", "yelping_since"
    ))
  )
}
