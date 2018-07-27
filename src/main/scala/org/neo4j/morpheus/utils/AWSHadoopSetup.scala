package org.neo4j.morpheus.utils

import org.apache.hadoop.conf.Configuration
import org.opencypher.spark.api.CAPSSession

object AWSHadoopSetup {
  def apply(bucketName: String)(implicit capsSession: CAPSSession): Unit = {
    // Note normally this configuration would already be setup on a live Hadoop cluster.
    val accessKey = sys.env.getOrElse("AWS_ACCESS_KEY_ID", sys.error("AWS_ACCESS_KEY_ID environment variable not set up"))
    val secretKey = sys.env.getOrElse("AWS_SECRET_ACCESS_KEY", sys.error("AWS_SECRET_ACCESS_KEY environment variable not set up"))
    val configuration: Configuration = capsSession.sparkSession.sparkContext.hadoopConfiguration
    configuration.set("fs.s3a.access.key", accessKey)
    configuration.set("fs.s3a.secret.key", secretKey)
    configuration.set("fs.defaultFS", s"s3a://$bucketName") // Set the default file system to S3 bucket
  }
}
