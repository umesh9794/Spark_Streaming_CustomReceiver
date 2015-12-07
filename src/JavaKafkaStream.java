import au.com.bytecode.opencsv.CSVReader;
import com.google.gson.Gson;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by user on 30/7/15.
 */
public class JavaKafkaStream {

    static  Map<String,Integer> aggregationMap=new ConcurrentHashMap<String,Integer>();
    static  Map<String,String> countryCodeMap=new ConcurrentHashMap<String,String>();
   // static  Map<String,Integer> websiteCoordMap=new ConcurrentHashMap<String,Integer>();
    static  Map<String,Integer> userMap=new ConcurrentHashMap<String,Integer>();

    public static void main(final String[] args) throws IOException {

//        args[0] : group.id
//        args[1] : topic Name
//        args[2] : directory path for text files

        //Writing into Log File
        File logfile = new File(args[4]);
        if (!logfile.exists())
            logfile.createNewFile();

        final Writer logewriter = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream(logfile), "utf-8"));

        final String fileName = args[2] + "/geoData.txt";
        String websiteStstFileName = args[3] + "/website_stat.txt";

       final File f = new File(fileName);
        final File websitef = new File(websiteStstFileName);
        final File userCount = new File(args[3] + "/"+"usercount.txt");


        //Writing Map Data
        if (!f.exists())
            f.createNewFile();

        //Writing Website Data
        if (!websitef.exists())
            websitef.createNewFile();

        //Writing UserCount Data
        if (!userCount.exists())
            userCount.createNewFile();



       final Gson gson=new Gson();

        BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream("/home/user/Downloads/country_code.csv"), "UTF-8"));
        CSVReader csvReader = new CSVReader(in);


        List<String[]> csvFile=csvReader.readAll();


        for(String[] s: csvFile)
        {
            countryCodeMap.put(s[0],s[1]+","+s[2]);
            aggregationMap.put(s[1]+","+s[2],0);
        }

        SparkConf sparkConf = new SparkConf().setAppName("KafkaMQStreaming").setMaster("local[2]");
        //    val sparkConf: SparkConf = new SparkConf().setAppName("WebSphereMQStreaming").set("spark.executor.memory","1g").setMaster("spark://inpunpc323423:7077")
         JavaStreamingContext ssc= new JavaStreamingContext(sparkConf, new Duration(Long.parseLong(args[5])));

        Map<String,String> kafkaparams= new HashMap<String,String>();
        kafkaparams.put("zookeeper.connect","172.29.80.75:2181");
        //kafkaparams.put("metadata.broker.list","node1:9092,node3:9092,node3:9092,node4:9092,node0:9092");
        kafkaparams.put("auto.offset.reset","smallest");
        kafkaparams.put("group.id",args[0]);
        
//        kafkaparams.put("dual.commit.enabled","true");
//
        Map<String,Integer> topics =new HashMap<String,Integer>();
        topics.put(args[1], 1);

//        Set<String> topic= new HashSet<String> ();
//        topic.add("heat-map-2");

//        JavaPairInputDStream<String, String> kafkastream =  KafkaUtils.createDirectStream(ssc,String.class,String.class, StringDecoder.class,StringDecoder.class,kafkaparams,topic);
      JavaPairInputDStream<String, String> kafkastream = KafkaUtils.createStream(ssc, String.class, String.class, StringDecoder.class,StringDecoder.class,kafkaparams,topics,StorageLevel.MEMORY_ONLY_2());

        JavaDStream<String> jsonData = kafkastream.map(
                new Function<Tuple2<String, String>, String>() {
                    public String call(Tuple2<String, String> message) {
                        return message._2();
                    }
                }
        );


        kafkastream.foreachRDD(new Function2<JavaPairRDD<String, String>, Time, Void>() {

            public Void call(JavaPairRDD<String, String> rdd, Time t) {
                 Map<String,Integer> websiteCoordMap=new ConcurrentHashMap<String,Integer>();
                StringBuilder sb = new StringBuilder();
                StringBuilder sbWebSite = new StringBuilder();
                List<String> userList=new ArrayList<String>();
                System.out.println("Records Processed : " + rdd.count());
                // rdd.values().saveAsTextFile("/home/user/kafka_out/out1"+t.toString());
                List<String> tuples = rdd.values().collect();

                if (tuples.size() > 0) {
                    for (String tuple : tuples) {
                        String[] splittedVals = tuple.split(",");
                        if (splittedVals.length == 5) {
                            String countryCode = splittedVals[3].toString();
                            // System.out.println("Country Code Is: " + countryCode);
                            String latLong = countryCodeMap.get(countryCode);
                            if (latLong != null) {
                                Integer requestCountPerCountry = aggregationMap.get(latLong);
                                aggregationMap.put(latLong, requestCountPerCountry + 1);
                                //sb.append(latLong + "\n");
                            }

                            String key = splittedVals[0] + "," + splittedVals[1];

                            //For HeatMap of Website Statistics
                            if (websiteCoordMap.containsKey(key)) {
                                Integer val = websiteCoordMap.get(key);
                                websiteCoordMap.put(key, val + 1);
                            } else
                                websiteCoordMap.put(key, 1);


                            //For Online User Count
                            if (!userList.contains(splittedVals[2])) {
                                    userList.add(splittedVals[2]);
                            }
                        }
                    }

                try {

                    System.out.println("Online Users: "+userList.size());
                    sbWebSite.append("day,hour,value\n");

                    for(String key : websiteCoordMap.keySet())
                    {
                        sbWebSite.append(key+ "," + websiteCoordMap.get(key) +"\n");
                    }

                    for (String key : aggregationMap.keySet()) {
                        sb.append(key + "," + aggregationMap.get(key) + "\n");
                    }

                     Writer writer = new BufferedWriter(new OutputStreamWriter(
                            new FileOutputStream(f,false), "utf-8"));

                    Writer filewriter = new BufferedWriter(new OutputStreamWriter(
                            new FileOutputStream(websitef,false), "utf-8"));

                    Writer usercountwriter = new BufferedWriter(new OutputStreamWriter(
                            new FileOutputStream(userCount,false), "utf-8"));

                    writer.write(sb.toString());
                    filewriter.write(sbWebSite.toString());
                    usercountwriter.write(String.valueOf(userList.size()));



                    logewriter.append(new SimpleDateFormat("MM-dd-yyyy HH:mm:ss").format(new Date()) + " : " + " Records Processed : " + rdd.count() + "\n");
                    logewriter.append("Online Users: " + userList.size() + "\n");
                    logewriter.flush();
                    filewriter.flush();
                    writer.flush();
                    usercountwriter.flush();

                    System.out.println("Data Written to File..." + fileName);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                // System.out.println(sb.toString());
            }
            return null;
        }
    }
            );

        ssc.start();
        ssc.awaitTermination();
    }

}
