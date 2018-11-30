package uniqueId

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level

object AssignUniqueIdMain {

  def main(args : Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nuniqueId.AssignUniqueIdMain <input dir> <output dir>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("assign-unique-id")
    val sc = new SparkContext(conf)

    // Delete output directory, only to ease local development; will not work on AWS. ===========
    val hadoopConf = new org.apache.hadoop.conf.Configuration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
    // ================

    val textFile = sc.textFile(args(0))
    val unique = textFile.zipWithUniqueId()
                         .map(x => (x._2, x._1))
                         .map(x => x.productIterator.toSeq.mkString(","))
    print(unique.toDebugString)
    unique.saveAsTextFile(args(1))
  }
}
