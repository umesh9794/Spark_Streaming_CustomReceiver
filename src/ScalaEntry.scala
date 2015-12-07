import java.security.MessageDigest
import java.sql.Timestamp
import java.util.UUID

import au.com.bytecode.opencsv.CSVParser
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, Minutes, StreamingContext, Duration}
import org.apache.spark.streaming.api.java.JavaStreamingContext
import org.apache.spark.{SparkContext, SparkConf}
import org.shc.spark.streaming.customreciever.CustomMQReciever
import shc.npos.parsers.MessageParser

/**
 * Created by user on 13/5/15.
 */
object ScalaEntry {

  def main(args: Array[String]): Unit =
  {

    //Setting Spark Config
    val conf = new SparkConf().setAppName("SimpleScala").setMaster("spark://inpunpc323423:7077")
      .set("spark.executor.memory", "2g")
    val sc=new SparkContext(conf)
    sc.addJar("/home/user/IdeaProjects/scala/out/artifacts/scala_jar/scala.jar");

    //Creating and caching RDD
    val csv= sc.textFile("hdfs://inpunpc310350:9000/user/scalaTest/customer.csv").cache()


    //Getting PID
    val PID = Starter.processId();
    //Getting MAC Address
    val MAC = Starter.macAddress();

    //RDD transformation for adding extra UUID column and masking Credit Card Number and Cell Number Column
    val data : RDD[(String,String,String,String)] = csv.map(line => {
      val parser = new CSVParser(',')
      val columns=  parser.parseLine(line)
      val md = MessageDigest.getInstance("MD5");
      //Creating UUID
      md.update((UUID.randomUUID().toString() + Integer.toString(PID) + MAC + new Timestamp(System.currentTimeMillis()).toString()).getBytes());
      (Starter.byteToHex(md.digest()),Starter.createMaskOnCreditCard(columns(0)),Starter.createMaskOnCellNum(columns(1)),columns(2))

    })

    //RDD Action for saving at HDFS
    data.saveAsTextFile("hdfs://inpunpc310350:9000/user/scalaTest/sparkOut")



  }

}
