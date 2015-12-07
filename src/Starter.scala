/**
 * Created by user on 12/5/15.
 */


import java.io.{InputStreamReader, BufferedReader, OutputStreamWriter, BufferedWriter}
import java.lang.NumberFormatException
import java.lang.management.ManagementFactory
import java.math.BigInteger
import java.net._
import java.security.MessageDigest
import java.sql.Timestamp
import java.util
import au.com.bytecode.opencsv.{CSVParser, CSVReader, CSVWriter}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import java.util.{Calendar, UUID}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

object Starter  {


  /**
   *
   * @param args
   */
  def main(args: Array[String]) {


    val config = new SparkConf().setAppName("SimpleScala").setMaster("spark://inpunpc323423:7077")
      .set("spark.executor.memory", "2g")
    val sc=new SparkContext(config)

    println("getting configuration..")

    val conf = new Configuration()
    conf.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName());
    conf.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName());

    val fs = FileSystem.get(new URI("hdfs://inpunpc310350:9000    "), conf)
    val path = new Path("/user/scalaTest/customer.csv")

    val output = fs.create(path)
    println("output file created...")
    val out = new BufferedWriter(new OutputStreamWriter(output, "UTF-8"));
    val writer = new CSVWriter(out);

      for (x <- 1 to 100000000) {
        val card = 222334455553321l;
        val cell = 9100911100l;

        val date = new java.util.Date().getDate();
        val employee = Array((card + 1).toString(), (cell + 1).toString(), new Timestamp(date + 1).toString());
      writer.writeNext(employee);
      if (x % 10000 == 0) println("Written " + x.toString() + "lines..");
    }
    writer.close()

    println("CSV File filled with data!");
  }


  /**
   *
   * @param creditCard
   * @return
   */
  def createMaskOnCreditCard(creditCard: String): String = {

    return creditCard.substring(0, creditCard.length() - 4).replaceAll("[0-9]", "X") + creditCard.substring(creditCard.length() - 4, creditCard.length())

  }

  /**
   *
   * @param cell
   * @return
   */
  def createMaskOnCellNum(cell: String): String = {
    return cell.substring(0, cell.length() - 4).replaceAll("[0-9]", "X") + cell.substring(cell.length() - 4, cell.length())
  }

  /**
   *
   */
  def test() = println("Called From Java Class")


  /**
   *
   */
  def createOutCSV(): Unit = {
    try {
    println("Execution  Started At : " + new Timestamp(System.currentTimeMillis()).toString)

    val conf = new Configuration()
    conf.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName());
    conf.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName());
    val fs = FileSystem.get(new URI("hdfs://inpunpc310350:9000"), conf)
    val path = new Path("/user/scalaTest/customer.csv")

    val in = new BufferedReader(new InputStreamReader(fs.open(path), "UTF-8"));

    val csvReader = new CSVReader(in);

    var nextLine = new Array[String](3);

    val md = MessageDigest.getInstance("MD5");

    val output = fs.create(new Path("/user/scalaTest/Final.csv"))
    val out = new BufferedWriter(new OutputStreamWriter(output, "UTF-8"));
    val writer = new CSVWriter(out);
    val employeeSchema=Array("CheckSum","CreditCard_Number","Cell_Number","Timestamp");
    writer.writeNext(employeeSchema)

    val array= csvReader.readAll()
    val PID = processId();
    val MAC = macAddress();

    for (i<- 0 until array.size())
    {
     // println(array.get(i)(0)+ "  "+ array.get(i)(1));

      val maskedCard = if (array.get(i)(0) != null) createMaskOnCreditCard(array.get(i)(0).toString) else "";
      val maskedCell = if (array.get(i)(1) != null) createMaskOnCellNum(array.get(i)(1).toString) else "";

              md.update((UUID.randomUUID().toString() + Integer.toString(PID) + MAC + new Timestamp(System.currentTimeMillis()).toString()).getBytes());
              writer.writeNext( Array(byteToHex(md.digest()), maskedCard, maskedCell, new Timestamp(System.currentTimeMillis()).toString ))

    }
    writer.close()


//      while ((nextLine = csvReader.readNext()) != null) {
//        var PID = processId();
//        var MAC = macAddress();
//        var maskedCard = if (nextLine(0) != null) createMaskOnCreditCard(nextLine(0).toString) else "";
//        var maskedCell = if (nextLine(1) != null) createMaskOnCellNum(nextLine(1).toString) else "";
//        md.update((UUID.randomUUID().toString() + Integer.toString(PID) + MAC + new Timestamp(System.currentTimeMillis()).toString()).getBytes());
//        //writer.writeNext( Array(byteToHex(md.digest()), maskedCard, maskedCell, new Timestamp(System.currentTimeMillis()).toString ))
//        writer.writeNext( Array(maskedCard, maskedCell, new Timestamp(System.currentTimeMillis()).toString ))        //println(byteToHex(md.digest()) + "~~" + PID + "~~" + MAC + "~~" + maskedCard + "~~" + maskedCell + "~~"+time.toString)
//      }
//      writer.close()
      println("Execution Ended At : " + new Timestamp(System.currentTimeMillis()).toString)
    }
      catch
      {
        case ex: Exception => ex.printStackTrace();

      }
    }


  /**
   *
   * @return
   */
  def processId(): Int = {
    val jvmName = ManagementFactory.getRuntimeMXBean().getName();
    val index = jvmName.indexOf('@');

    if (index < 1)
      throw new RuntimeException("Could not get PID");

    try {
      return Integer.parseInt(jvmName.substring(0, index)) % 65536;
    } catch {
      case ex: NumberFormatException =>
        throw new RuntimeException("Could not get PID");
    }
  }


  /**
   *
   * @return
   */
  def macAddress(): String = {

    val sb = new StringBuilder();
    try {

      val ip = InetAddress.getLocalHost();
      //System.out.println("Current IP address : " + ip.getHostAddress());

      val network = NetworkInterface.getByInetAddress(ip);

      val mac = network.getHardwareAddress();


      for (i <- 0 until mac.length) {
        sb.append("%02X%s".format( mac(i), if (i < mac.length - 1) "-" else ""));
      }

    } catch {

      case ex: UnknownHostException =>
        ex.printStackTrace();

      case ex: SocketException =>
        ex.printStackTrace();
    }

    return sb.toString();
  }

  /**
   *
   * @param x
   * @return
   */
  def byteToHex(x: Array[Byte]): String = {
    val hexString = new StringBuffer();
    for (i <- 0 until x.length) {
      val hex = Integer.toHexString(0xff & x(i));
      if (hex.length() == 1) hexString.append('0');
      hexString.append(hex);
    }
    return hexString.toString;

  }

  /**
   *
   */
  def processCSV(): Unit =
  {

    val conf = new SparkConf().setAppName("SimpleScala").setMaster("spark://inpunpc323423:7077")
              .set("spark.executor.memory", "2g")
    val sc=new SparkContext(conf)
     sc.addJar("/home/user/IdeaProjects/scala/out/artifacts/scala_jar/scala.jar");

    val csv= sc.textFile("hdfs://inpunpc310350:9000/user/scalaTest/customer.csv").cache()


    val PID = processId();
    val MAC = macAddress();

    val data : RDD[(String,String,String,String)] = csv.map(line => {
      val parser = new CSVParser(',')
      val columns=  parser.parseLine(line)
      val md = MessageDigest.getInstance("MD5");
      md.update((UUID.randomUUID().toString() + Integer.toString(PID) + MAC + new Timestamp(System.currentTimeMillis()).toString()).getBytes());
      (byteToHex(md.digest()),createMaskOnCreditCard(columns(0)),createMaskOnCellNum(columns(1)),columns(2))

    })

     data.saveAsTextFile("hdfs://inpunpc310350:9000/user/scalaTest/sparkOut")

  }

}




