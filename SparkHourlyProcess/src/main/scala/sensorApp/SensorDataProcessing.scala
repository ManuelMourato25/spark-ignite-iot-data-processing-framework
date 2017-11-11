package sensorApp

/**
  * Created by manuel.mourato on 04-10-2017.
  */


import org.apache.ignite.cache.{CacheAtomicityMode, CacheMode}
import org.apache.ignite.configuration.{CacheConfiguration, IgniteConfiguration}
import org.apache.ignite.spark.{IgniteContext, IgniteRDD}
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import java.util
import org.apache.ignite.cache.query.annotations.QuerySqlField
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi
import scala.annotation.meta.field

//This is a Spark Application meant to show the Shared RDD and RDD mutability paradigm
//That Ignite brings. It is meant to run in an hourly manner, with something like Cron

object SensorDataProcessing {

  case class Sensor_Att(
                         @(QuerySqlField @field)(index = true)    active: String,
                         @(QuerySqlField @field)(index = true)    `type`: String,
                         @(QuerySqlField @field)(index = true)    name: String,
                         @(QuerySqlField @field)(index = true)    coordinates: String,
                         @(QuerySqlField @field)(index = true)    Temperature: String,
                         @(QuerySqlField @field)(index = true)    Rain: String,
                         @(QuerySqlField @field)(index = true)    Pressure: String,
                         @(QuerySqlField @field)(index = true)    Humidity: String,
                         @(QuerySqlField @field)(index = true)    Visibility: String,
                         @(QuerySqlField @field)(index = true)    CO: String,
                         @(QuerySqlField @field)(index = true)    Sound: String,
                         @(QuerySqlField @field)(index = true)    NO: String,
                         @(QuerySqlField @field)(index = true)    NO2: String,
                         @(QuerySqlField @field)(index = true)    sensor_height:String,
                         @(QuerySqlField @field)(index = true)    source_name:String,
                         @(QuerySqlField @field)(index = true)    db_name:String,
                         @(QuerySqlField @field)(index = true)    document:String,
                         @(QuerySqlField @field)(index = true)    webDisplay:String,
                         @(QuerySqlField @field)(index = true)    third_party:String,
                         @(QuerySqlField @field)(index = true)    Wind_Gust:String,
                         @(QuerySqlField @field)(index = true)    Wind_Direction:String,
                         @(QuerySqlField @field)(index = true)    Wind_Speed:String

                       )

  def main(args: Array[String]): Unit = {

    val sqlContext: SparkSession = SparkSession.builder().master("local[*]").appName("SensorDataProcessing").getOrCreate()
    val sc: SparkContext = sqlContext.sparkContext

    sc.setLogLevel("ERROR")

    val ignitec:IgniteContext = new IgniteContext(sc,()=>new IgniteConfiguration().setClientMode(false).setLocalHost("127.0.0.1").setActiveOnStart(true).
      setCacheConfiguration(new CacheConfiguration[String,Sensor_Att]().setIndexedTypes(classOf[String],classOf[Sensor_Att]).setName("sensorData").
        setCacheMode(CacheMode.PARTITIONED).setAtomicityMode(CacheAtomicityMode.ATOMIC)).setDiscoverySpi(new TcpDiscoverySpi().
      setLocalAddress("127.0.0.1").setLocalPort(47097).setIpFinder(new TcpDiscoveryMulticastIpFinder().
      setAddresses(new util.ArrayList[String]))).setCommunicationSpi(new TcpCommunicationSpi().setLocalPort(47007)))

    //In this App the goal is to store the hourly data collected in our streaming app
    // and to clean the Ignite cache afterwards so as to avoid over using resources

    val hourly_data:IgniteRDD[String,Sensor_Att]=ignitec.fromCache[String,Sensor_Att]("sensorData")
    hourly_data.sql("select * from Sensor_Att").toJSON.rdd.saveAsTextFile("/home/manuelmourato/Personal/hourly_data.csv")
    hourly_data.clear()
    ignitec.close(true)
    sc.stop()



  }
}
