import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.streaming._

object advanced_stream {

  def main(args: Array[String]) {

    Logger.getRootLogger().setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("webLogStream").setMaster("local[2]")
    val stc = new StreamingContext(conf, Seconds(5))
    stc.checkpoint("checkpoint")
    val stream = stc.socketTextStream("localhost", 33001)
    stream.print()

    val columns = stream.map(_.split("\n"))
    columns.print()
    println("___________")
    val result = columns.map(_.mkString(""))
    println("Converted to string.")
    result.print()
    // making new rdd from news
    val rdd = stc.sparkContext.parallelize("result")
    //finding visit rate equals or greater than 30% among other news and printing them
    if (rdd.foreach(x => x > x / 100 * 7)) println(rdd.collect()) else 0


    println("SUCCESS_")

    stc.start()
    stc.awaitTermination()

  }
}