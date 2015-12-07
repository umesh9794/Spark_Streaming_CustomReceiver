import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Duration, StreamingContext}

/**
 * Created by user on 30/7/15.
 */
object KafkaStreaming {

  def main (args: Array[String]){


    val sparkConf: SparkConf = new SparkConf().setAppName("KafkaMQStreaming").setMaster("local[3]")
    //    val sparkConf: SparkConf = new SparkConf().setAppName("WebSphereMQStreaming").set("spark.executor.memory","1g").setMaster("spark://inpunpc323423:7077")
    val ssc: StreamingContext = new StreamingContext(sparkConf, new Duration(1000))

    val kafkaparams= Map("metadata.broker.list"-> "172.29.95.104:2181")

    val topics = Set("heat-map-2")
//    val topics= Map("heat-map-2"-> 1)

val numPartitionsOfInputTopic = 5
    val streams = (1 to numPartitionsOfInputTopic) map { _ =>
      KafkaUtils.createStream(ssc, kafkaParams, Map(inputTopic -> 1), StorageLevel.MEMORY_ONLY_SER).map(_._2)
    }


  }



  }
