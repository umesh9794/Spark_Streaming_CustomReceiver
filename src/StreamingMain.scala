
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{ Duration, StreamingContext}
import org.shc.spark.streaming.customreciever.CustomMQReciever


/**
 * Created by user on 10/6/15.
 */
object StreamingMain {

  def main (args: Array[String]) {
    val sparkConf: SparkConf = new SparkConf().setAppName("WebSphereMQStreaming").set("spark.executor.memory","1g").setMaster("spark://inpunpc323423:7077")
//    val sparkConf: SparkConf = new SparkConf().setAppName("WebSphereMQStreaming").set("spark.executor.memory","1g").setMaster("spark://inpunpc323423:7077")
    val ssc: StreamingContext = new StreamingContext(sparkConf, new Duration(1000))


    //val kafkaparams= Map("metadata.broker.list"-> "inpunpc310350:9092")

    //val topics= Map("test"-> 1)

    //val kafkastream = KafkaUtils.createStream( ssc,"inpunpc310350:2181","",topics, StorageLevel.DISK_ONLY_2)

    //ssc.sparkContext.addJar("/home/user/IdeaProjects/scala/out/artifacts/scala_jar/scala.jar")
    val numPart= 1;

    //kafkastream.print()

    val customReceiverStream =(1 to numPart).map{t=> ssc.receiverStream(new CustomMQReciever("hofmdsappstrs1.sears.com", 1414, "SQAT0263", "STORM.SVRCONN", "MDS0.STORM.RFID.PILOT.QL01"))}

    val unified= ssc.union(customReceiverStream)

    unified.repartition(3);
    //val windowedDstream= customReceiverStream.countByWindow(Minutes(1), Seconds(3))
    //customReceiverStream.saveAsTextFiles("/home/user/streamOut")

   // customReceiverStream.print

    unified.foreachRDD(t=> {

   // val segmentCount=  t.filter(t=>t.contains("Transfer")).count()
      val string = t.collect()

      string.foreach(t=> println( "Actual String :"+ t +"\n"+  "Type in String :"+ t.split(" ").count(t=>t.equals("Type")) + "\n"))

      //println("Count of Transfer Words :"+ t.collect())

    })

    ssc.start
    ssc.awaitTermination

  }

}
