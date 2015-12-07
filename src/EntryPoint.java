import com.google.common.collect.Lists;
import com.google.gson.reflect.TypeToken;
import org.apache.spark.SparkConf;
import org.apache.spark.rdd.RDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.DStream;
import org.shc.spark.streaming.customreciever.CustomMQReciever;
import org.shc.spark.streaming.customreciever.WebsphereMQReciever;
import shc.npos.parsers.MessageParser;

import javax.jms.JMSException;
import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * Created by user on 12/5/15.
 */

public class EntryPoint implements Serializable {



    public static void main(String[] args) {

       // Starter.main(null);

       // Starter.processCSV();



        try {
            //ScalaEntry.main(null);

            SparkConf sparkConf = new SparkConf().setAppName("TestMQStreaming").setMaster("local[2]");

            JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(1000));

             JavaDStream<String> customReceiverStream = ssc.receiverStream(new CustomMQReciever("hofmdsappstrs1.sears.com", 1414, "SQAT0263", "STORM.SVRCONN", "MDS0.STORM.RFID.PILOT.QL01"));


//            DStream<String> words = customReceiverStream.flatMap(new FlatMapFunction<String, String>() {
//                public Iterable<String> call(String x) {
//                    System.out.println("Message received" + x);
//                    return Lists.newArrayList(x);
//                }
//            });

            customReceiverStream.print();
            ssc.start();
            ssc.awaitTermination();

            //CustomMQReciever mq = new CustomMQReciever("hfqamqsvr3.vm.itg.corp.us.shldcorp.com", 1414, "SQCT0001", "STORM.SVRCONN", "STORM.QA.EES.DATACOLLECT.QC01");

            //mq.initConnection();
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
        }
        

    }

}