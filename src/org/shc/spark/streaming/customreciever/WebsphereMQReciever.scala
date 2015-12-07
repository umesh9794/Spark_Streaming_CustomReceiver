package org.shc.spark.streaming.customreciever

import java.io.{BufferedReader, InputStreamReader}

import com.ibm.jms.JMSMessage
import com.ibm.mq.jms._
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

/**
 * Created by user on 26/5/15.
 */
class WebsphereMQReciever(host: String, port: Int, qm: String, channel: String, qname: String) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging{

  //var reciever=new MQQueueReceiver()

  def onStart() {
    // Start the thread that receives data over a connection
    new Thread("Socket Receiver") {
      override def run() { receive() }
    }.start()
  }

  def onStop() {
    // There is nothing much to do as the thread calling receive()
    // is designed to stop by itself isStopped() returns false
  }

  /** Create a socket connection and receive data until receiver is stopped */
  private def receive() {
    initConnection();

//    var socket: Socket = null
    var userInput: String = null
    try {
      // Connect to host:port
      //val mqMessage= reciever.receive().asInstanceOf[JMSMessage]

      // Until stopped or connection broken continue reading
      //val reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), "UTF-8"))
//      userInput = reader.readLine()
//      while(!isStopped && userInput != null) {
//        store(userInput)
//        userInput = reader.readLine()
//      }
//      reader.close()
//      socket.close()

      // Restart in an attempt to connect again when server is active again
      restart("Trying to connect again")
    } catch {
      case e: Exception=>
        // restart if could not connect to server
        restart("Error connecting to " + host + ":" + port, e)
      case t: Throwable =>
        // restart if there is any other error
        restart("Error receiving data", t)
    }
  }

  def initConnection(): Unit =
  {
    val conFactory= new MQQueueConnectionFactory();
    conFactory.setHostName(host)
    conFactory.setPort(port)
    conFactory.setTransportType(JMSC.MQJMS_TP_CLIENT_MQ_TCPIP)
    conFactory.setQueueManager(qm)
    conFactory.setChannel(channel)
    var queue= new MQQueue();

    val qCon=  conFactory.createQueueConnection().asInstanceOf[MQQueueConnection];
    val qSession= qCon.createQueueSession(false, 1).asInstanceOf[MQQueueSession];
          queue= qSession.createQueue(qname).asInstanceOf[MQQueue];
      //reciever = qSession.createReceiver(queue.asInstanceOf[Nothing]).asInstanceOf[MQQueueReceiver]

    qCon.start()



  }

}
