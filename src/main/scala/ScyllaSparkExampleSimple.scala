import java.util.UUID

import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.parsing.json._
import org.joda.time.{DateTime, _}


object ScyllaSparkExampleSimple {

    def main(args: Array[String]): Unit = {


//    go()
    goRead()
//        goInsert()
    }

    def go(): Unit = {
        val STEP = 10000
        val LIMIT = 1000000
        var currentStep = 190000

        var result = new ArrayBuffer[String]()
        result += s"for:___, took:___"



        var data = new ListBuffer[Tuple2[UUID, String]]
        data = data ++ createSampleJson11(currentStep)


        var conf = new SparkConf();
        conf.set("spark.cassandra.read.timeout_ms", "9000000000");
        conf.set("spark.cassandra.connection.timeout_ms", "9000000000");
        conf.set("spark.cassandra.connection.keep_alive_ms", "9000000000");
        conf.set("spark.cassandra.output.batch.size.rows", "1");
        conf.set("spark.cassandra.output.concurrent.writes", "1");
        conf.set("spark.cassandra.output.throughput_mb_per_sec", "1");
        conf.set("spark.cassandra.output.throughput_mb_per_sec", "1");





        var sc = new SparkContext(new SparkConf())


        while(currentStep < LIMIT) {
            currentStep += STEP

            data = data ++ createSampleJson11(STEP)


            val start = DateTime.now
            val collection = sc.parallelize(data)


            collection.saveToCassandra("spark_example", "json_test")
            val end = DateTime.now


            result += s"${data.length},${Seconds.secondsBetween(start, end).getSeconds()}"

            println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
            result.foreach(println)
            println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        }


    }

    def goRead(): Unit = {
        val STEP = 10000
        val LIMIT = 1000000
        var currentStep = 0

        var result = new ArrayBuffer[String]()
        result += s"for:___, took:___"



        var data = new ListBuffer[Tuple2[UUID, String]]


        var conf = new SparkConf();
        conf.set("spark.cassandra.read.timeout_ms", "9000000000");
        conf.set("spark.cassandra.connection.timeout_ms", "9000000000");
        conf.set("spark.cassandra.connection.keep_alive_ms", "9000000000");
        conf.set("spark.cassandra.output.batch.size.rows", "1");
        conf.set("spark.cassandra.output.concurrent.writes", "1");
        conf.set("spark.cassandra.output.throughput_mb_per_sec", "1");
        conf.set("spark.cassandra.output.throughput_mb_per_sec", "1");





        var sc = new SparkContext(new SparkConf())


        while(currentStep < LIMIT) {
            currentStep += STEP



            val start = DateTime.now

            val myTableData = sc.cassandraTable("spark_example", "json_test")
            val adults = myTableData.limit(currentStep)

            adults.collect()
            val end = DateTime.now


            result += s"${adults.count()},${Seconds.secondsBetween(start, end).getSeconds()}"

            println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
            result.foreach(println)
            println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        }


    }

    def goInsert(): Unit = {
        var sc = new SparkContext(new SparkConf())

        for(i <- 1 to 20) {
            var data = new ListBuffer[Tuple2[UUID, String]]
            data = createSampleJson11(10000)

            val collection = sc.parallelize(data)


            collection.saveToCassandra("spark_example", "json_test")
        }

    }

    def createSampleJson11(count: Int): ListBuffer[Tuple2[UUID, String]] = {
        var jsonList = new ListBuffer[Tuple2[UUID, String]]()

        var json = """ {"jkjkjkjkjkkj": ["jkakjsd/asdasd/adadasda"], "iiuiuiuiuiuiu": "2shhwrjkhwekjiiuuihhiuiuhhiuhiuiuh", "hjkashjkdasd": [{"iiiier": "jjkjks/sdfsdf/rwr", "shjhjhjhjtars": 3}]} """

        for (i <- 1 to count) {
            jsonList += Tuple2(UUID.randomUUID(), json)
        }

        return jsonList
    }


}