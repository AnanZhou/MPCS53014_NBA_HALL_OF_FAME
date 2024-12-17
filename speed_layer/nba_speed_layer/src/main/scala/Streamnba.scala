import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Increment, Table}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.util.Bytes

object NBAPlayerStatsConsumer {
  // HBase Configuration
  val hbaseConf = HBaseConfiguration.create()
  val connection = ConnectionFactory.createConnection(hbaseConf)
  val tableName = "zhoua_player_summary_hbase"
  val playerTable: Table = connection.getTable(TableName.valueOf(tableName))

  // Extract Player Name and Total Points from Kafka Message
  def parseKafkaMessage(message: String): (String, Long) = {
    val pattern = "Total Stats for (.+) in Season.*Points: (\\d+),.*".r
    message match {
      case pattern(playerName, totalPoints) => (playerName, totalPoints.toLong)
      case _ =>
        throw new IllegalArgumentException(s"Unexpected message format: $message")
    }
  }

  // Update HBase Table
  def updateHBase(playerName: String, totalPoints: Long): Unit = {
    // Get existing points
    val get = new Get(Bytes.toBytes(playerName))
    get.addColumn(Bytes.toBytes("details"), Bytes.toBytes("current_total_points"))

    val result = playerTable.get(get)
    val existingPoints = if (!result.isEmpty) {
      Bytes.toLong(result.getValue(Bytes.toBytes("details"), Bytes.toBytes("current_total_points")))
    } else {
      0L
    }

    // Increment points
    val increment = new Increment(Bytes.toBytes(playerName))
    increment.addColumn(Bytes.toBytes("details"), Bytes.toBytes("current_total_points"), totalPoints)
    playerTable.increment(increment)

    println(s"Updated $playerName: Total Points = ${existingPoints + totalPoints}")
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: NBAPlayerStatsConsumer <brokers>")
      System.exit(1)
    }

    val brokers = args(0)
    val sparkConf = new SparkConf().setAppName("NBAPlayerStatsConsumer").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(180))

    // Kafka parameters and topic subscription
    //consume all messages from the beginning of the topic: change into 'earliest' (auto.offset.reset)
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "nba-stats-group",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = Array("current-season-stats-zhoua")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc, PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    // Process Kafka messages
    stream.map(_.value()).foreachRDD { rdd =>
      rdd.foreach { message =>
        try {
          val (playerName, totalPoints) = parseKafkaMessage(message)
          updateHBase(playerName, totalPoints)
        } catch {
          case e: Exception => println(s"Failed to process message: $message. Error: ${e.getMessage}")
        }
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
