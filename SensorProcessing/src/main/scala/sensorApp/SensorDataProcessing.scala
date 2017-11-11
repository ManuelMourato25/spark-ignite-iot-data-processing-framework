package sensorApp

/**
  * Created by manuel.mourato on 04-10-2017.
  */

// In this script, I present a way to ingest JSON messages from a kafka source, process it
// in a Spark Streaming Context, and finally save it to an Ignite Cache for fast SQL querying
// and shared RDD capabilities

import net.liftweb.json._
import org.apache.ignite.cache.{CacheAtomicityMode, CacheMode}
import org.apache.ignite.configuration.{CacheConfiguration, IgniteConfiguration}
import org.apache.ignite.spark.{IgniteContext, IgniteRDD}
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.util
import org.apache.ignite.cache.query.annotations.QuerySqlField
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi
import scala.annotation.meta.field


object SensorDataProcessing {

//The use for this class will become more apparent later on.
//In general terms, this is where the Ignite table fiels need
//to be specified, according to the available JSON fields from the source data
 
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

//Starting Spark, Spark Streaming and Spark SQL contexts

    val sqlContext: SparkSession = SparkSession.builder().master("local[*]").appName("SensorDataProcessing").getOrCreate()
    val sc: SparkContext = sqlContext.sparkContext

    sc.setLogLevel("ERROR")

    val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))

//Starting the Ignite context with the necessary configurations
//Specifically, this where we can define our ignite cache/table name, as well
//as the key-value pair data types for this table

    val ignitec:IgniteContext = new IgniteContext(sc,()=>new IgniteConfiguration().setClientMode(false).setLocalHost("127.0.0.1").setActiveOnStart(true).
      setCacheConfiguration(new CacheConfiguration[String,Sensor_Att]().setIndexedTypes(classOf[String],classOf[Sensor_Att]).setName("sensorData").
        setCacheMode(CacheMode.PARTITIONED).setAtomicityMode(CacheAtomicityMode.ATOMIC)).setDiscoverySpi(new TcpDiscoverySpi().
      setLocalAddress("127.0.0.1").setLocalPort(47090).setIpFinder(new TcpDiscoveryMulticastIpFinder().
      setAddresses(new util.ArrayList[String]))).setCommunicationSpi(new TcpCommunicationSpi().setLocalPort(47000)))

//Definition of the used kafka source

    val broker = "localhost:2181"
    val topic = "sensorData"


//The schema for the received JSON messages

    val sensor_message_schema: StructType = new StructType(Array(
        StructField("active", StringType, true),
        StructField("type", StringType, true),
        StructField("name", StringType, true),
        StructField("geom", StructType(List(
          StructField("coordinates", StringType, true),
          StructField("type", StringType, true)
        )
        ), true),

        StructField("data", StructType(List(
          StructField("Wind Gust", StructType(List(
            StructField("meta", StructType(List(
              StructField("units", StringType, true),
              StructField("name", StringType, true),
              StructField("theme", StringType, true)
            )), true),
            StructField("data", StringType, true))), true),
          StructField("Temperature", StructType(List(
            StructField("meta", StructType(List(
              StructField("units", StringType, true),
              StructField("name", StringType, true),
              StructField("theme", StringType, true)
            )), true),
            StructField("data", StringType, true))), true),
          StructField("Rain", StructType(List(
            StructField("meta", StructType(List(
              StructField("units", StringType, true),
              StructField("name", StringType, true),
              StructField("theme", StringType, true)
            )), true),
            StructField("data", StringType, true))), true),
          StructField("Wind Direction", StructType(List(
            StructField("meta", StructType(List(
              StructField("units", StringType, true),
              StructField("name", StringType, true),
              StructField("theme", StringType, true)
            )), true),
            StructField("data", StringType, true))), true),
          StructField("Wind Speed", StructType(List(
            StructField("meta", StructType(List(
              StructField("units", StringType, true),
              StructField("name", StringType, true),
              StructField("theme", StringType, true)
            )), true),
            StructField("data", StringType, true))), true),
          StructField("Pressure", StructType(List(
            StructField("meta", StructType(List(
              StructField("units", StringType, true),
              StructField("name", StringType, true),
              StructField("theme", StringType, true)
            )), true),
            StructField("data", StringType, true))), true),
          StructField("Humidity", StructType(List(
            StructField("meta", StructType(List(
              StructField("units", StringType, true),
              StructField("name", StringType, true),
              StructField("theme", StringType, true)
            )), true),
            StructField("data", StringType, true))), true),
          StructField("NO2", StructType(List(
            StructField("meta", StructType(List(
              StructField("units", StringType, true),
              StructField("name", StringType, true),
              StructField("theme", StringType, true)
            )), true),
            StructField("data", StringType, true))), true),
          StructField("NO", StructType(List(
            StructField("meta", StructType(List(
              StructField("units", StringType, true),
              StructField("name", StringType, true),
              StructField("theme", StringType, true)
            )), true),
            StructField("data", StringType, true))), true),
          StructField("CO", StructType(List(
            StructField("meta", StructType(List(
              StructField("units", StringType, true),
              StructField("name", StringType, true),
              StructField("theme", StringType, true)
            )), true),
            StructField("data", StringType, true))), true),
          StructField("Sound", StructType(List(
            StructField("meta", StructType(List(
              StructField("units", StringType, true),
              StructField("name", StringType, true),
              StructField("theme", StringType, true)
            )), true),
            StructField("data", StringType, true))), true),
          StructField("Visibility", StructType(List(
            StructField("meta", StructType(List(
              StructField("units", StringType, true),
              StructField("name", StringType, true),
              StructField("theme", StringType, true)
            )), true),
            StructField("data", StringType, true))), true))),
          true),
        StructField("sensor_height", StringType, true),
        StructField("source", StructType(List(
          StructField("fancy_name",StringType,true),
          StructField("db_name",StringType,true),
          StructField("document",StringType,true),
          StructField("web_display_name",StringType,true),
          StructField("third_party",StringType,true)
        )), true)))



    val kafkaSensorData: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc, broker, "streaming-consumer", Map(topic -> 1))

    // A function that makes use of the liftweb library to convert JSON messages into a class
    def json_to_Class(jsonString:String): Sensor_Att = {

      implicit val formats = net.liftweb.json.DefaultFormats
      val json: JValue = parse(jsonString)
      val newClass:Sensor_Att=json.extract[Sensor_Att]
      return  newClass
    }


    // This is the data pocessing function that I want to apply to all the RDDs in my DStream
    def data_processing(rdd: RDD[String]): Unit = {

      var initial_processed_RDD: RDD[String] = rdd.map(x => x.replace("'", "\""))
      // This section could probably be replaced with just one regex like I did below...
      initial_processed_RDD = initial_processed_RDD.map(x => x.replace(", u\"", ",\""))
      initial_processed_RDD = initial_processed_RDD.map(x => x.replace(": u\"", ": \""))
      initial_processed_RDD = initial_processed_RDD.map(x => x.replace("{u\"", "{\""))
      initial_processed_RDD = initial_processed_RDD.map(x => x.replace("[u'[", ""))
      initial_processed_RDD = initial_processed_RDD.map(x => x.replace("]']", ""))
      initial_processed_RDD = initial_processed_RDD.map(x => x.replace("\"third_party\": False", "\"third_party\": \"False\""))
      initial_processed_RDD = initial_processed_RDD.map(x => x.replace("\"third_party\": True", "\"third_party\": \"True\""))
      initial_processed_RDD = initial_processed_RDD.map(x => x.replace("\"document\": None", "\"document\": \"None\""))

      val sensors_df: DataFrame = sqlContext.read.schema(sensor_message_schema).json(initial_processed_RDD)

      val df_attributes: DataFrame = sensors_df.selectExpr("active", "type", "name", "geom.coordinates as coordinates", "data.Temperature.data as Temperature",
        "data.Rain.data as Rain", "data.Pressure.data as Pressure",
        "data.Humidity.data as Humidity", "data.Visibility.data as Visibility", "data.CO.data as CO", "data.Sound.data as Sound", "data.NO.data as NO",
        "data.NO2.data as NO2", "sensor_height", "source.fancy_name as source_name","source.db_name as db_name","source.document as document",
        "source.web_display_name as webDisplay","source.third_party as third_party",
        "data.`Wind Gust`.data as Wind_Gust", "data.`Wind Direction`.data as Wind_Direction", "data.`Wind Speed`.data as Wind_Speed")

      //Make sure all fields appear in the dataframe, for the class conversion to work
      val df_final:DataFrame=df_attributes.na.fill("no_value")

      val df_to_RDD:RDD[String]=df_final.toJSON.rdd.map(x=>x.replace("\\","").replace("\"{","{").replace("}\"","}").replaceAll("""(?<=:)\{\".*?\:(\d+\.\d+)\}""","$1").
      replaceAll("""(?<=:)(\d)""","\"$1").replaceAll("""(\d(?=,\"))""","$1\"").replaceAll("""(\d(?=}))""","$1\""))

      //This creates an RDD with a class that is an index for the previously created Ignite cache
      val df_RDD_NEW_CLASS:RDD[Sensor_Att]=df_to_RDD.map(x=>json_to_Class(x))

      val cachedRDD:IgniteRDD[String,Sensor_Att]=ignitec.fromCache("sensorData")
      //Idealy here a timestamp key would be better, but the JSON message itself
      //did not have that field. I thought about using each sensor name as key
      //but then the historical data would be lost if they were rewritten
      val  RDD_with_key: RDD[(String, Sensor_Att)] =df_RDD_NEW_CLASS.map(x=>(x.hashCode().toString,x))
      cachedRDD.savePairs(RDD_with_key)
      val df=cachedRDD.sql("select * from Sensor_Att")
      df.show()
    }

    val kafkaSensorData_values: DStream[String] = kafkaSensorData.map(x => x._2)

    kafkaSensorData_values.foreachRDD(x => data_processing(x))

    ssc.start()
    ssc.awaitTermination()
  }
}