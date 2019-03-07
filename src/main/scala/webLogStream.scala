package webLogStream

import java.io.File

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger, LogManager}
import org.apache.spark.streaming._
import org.apache.spark.sql.functions._
import java.util.Arrays.toString
import org.apache.spark.sql.functions.split


object webLogStream {

  println("Object webLogStream Executed")
  println("-----")


  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("webLogStream").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(4))
    // val sc = new SparkContext(conf)
    // val sqlContext = new SQLContext(sc)

    // Find how many times distinct API version is requested and print it on a screen.
    val requests = ssc.socketTextStream("localhost", 33001)
    val columns = requests.map(_.split(" "))
    //     val result = columns.map(_.mkString("[", ", ", "]")).map(a => a.split(","))
    val result = columns.map(_.mkString("[", ", ", "")).map(res => res.split(", ")(2).split("/")(2)).map(string => (string,1)).reduceByKey(_+_)
    println("Converted to string. API version is requested and print it on a screen")
    result.print()

    /*
RESULT:
-------------------------------------------
Time: 1551970544000 ms
-------------------------------------------
(2.3,8)
(3.0,4)

     */

    val userlist = columns.map(g => (g(0), g(1))).transform(rdd => rdd.distinct().map{ case (a,b) => (b,1)})
    val readylist = userlist.reduceByKey(_+_).cache()
    val filtered = readylist.filter(res => res._2 > 1)
    filtered.print()

    /*
RESULT:
-------------------------------------------
Time: 1551970544000 ms
-------------------------------------------
(397832,2)
 */

    ssc.start()
    ssc.awaitTermination()

    println("We did it to last stage in main().")
  }
}

