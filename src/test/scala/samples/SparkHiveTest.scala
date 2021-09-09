package samples
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.{BeforeAndAfterAll, PrivateMethodTester}


class SparkHiveTest extends AnyFunSpec with BeforeAndAfterAll with PrivateMethodTester {

  System.setProperty("hadoop.home.dir", "c://hadoop//")


  describe("Spark Hive Unit Testing") {

    it("Spark Hive testing") {
      val sparkConf = new SparkConf().
        setMaster("local[*]").
        setAppName("test").
        set("spark.ui.enabled", "false").
        set("spark.app.id", "SparkHiveTests").
        set("spark.driver.host", "localhost").
        set(org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION.key, "hive")

      val sparkContext = new SparkContext(sparkConf)


      val sparkSession = SparkSession.builder
        .config(sparkConf)
        .enableHiveSupport()
        .getOrCreate()

      val schema = StructType(Array(
        StructField("sparkKey", StringType, true),
        StructField("sparkValue", StringType, true)
      ))

      val rdd = sparkContext.parallelize(Seq(Row("sk1", "sv1"), Row("sk2", "sv2"), Row("sk3", "sv3"), Row("sk4", "sv4")))
      val dataFrame: DataFrame = sparkSession.createDataFrame(rdd, schema)
      dataFrame.write.format("hive").mode("Overwrite").saveAsTable("test_hivetable")
      val df = sparkSession.sql("select * from test_hivetable")
      df.show()
      val count = df.count()
      println("count: " + count)
      assertResult(4)(count)
    }
  }

}














//1)
//dataFrame.createOrReplaceTempView("test_sparktable")
/*sparkSession.sql("drop table if exists test_hivetable")
sparkSession.sql("create table test_hivetable as select * from test_sparktable")*/

//2)
/*def enableHiveSupport: SparkSession.Builder = {
      override def conf= super.conf.set(org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION.key, "hive")
      //val hive="hive"
      //org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION.key=hive
    }
*/

//3)

/*
val sparkSession = SparkSession.builder
.master(master)
.config(sparkConf)
//.config("hive.metastore.warehouse.dir", "/tmp/hive/warehouse")
//.config("spark.sql.hive.hiveserver2.jdbc.url", "jdbc:derby:metastore_db;create=true")
//.enableHiveSupport()
.getOrCreate()*/

