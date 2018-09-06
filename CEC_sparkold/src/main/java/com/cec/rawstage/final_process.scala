package com.cec.rawstage
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import java.sql.Timestamp
import java.util.Date
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.SparkConf
import java.text.SimpleDateFormat
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.hadoop.fs._
import scala.collection.Seq
import java.net.URI

object final_process {

  /**
   * Method creates present system date in yyyy-MM-dd HH:mm:ss.SS for metadata curated file.
   * @return String
   */
  def PresentDate(): String =
    {
      val d: Timestamp = new Timestamp(System.currentTimeMillis())
      val formattter: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SS")
      val TimeStamp = formattter.parse(d.toString()).toString()
      return TimeStamp
    }

  /**
   * Main Method for this class curated to proposed processing file.
   * @argument 7
   * @argument01: cis_file file path
   * @argument02: imd files path
   * @argument03: sce_qfer file path
   * @argument04: pge_qfer file path
   * @argument05: final_cis_imd merged output file path
   * @argument06:	final_qfer_out merged output file path
   * @argument07:	final_cis_imd_qfer_out merged output file path
   * @return Unit
   */

  def main(args: Array[String]): Unit = {

    val cis_file = args(0)
    val imd_file = args(1)
    val sce_qfer_path = args(2)
    val pge_qfer_path = args(3)
    var final_cis_imd_out = args(4)
    var final_qfer_out = args(5)
    var final_cis_imd_qfer_out = args(6)

    //    val cis_file = "C:\\CEC_Documents\\Curated\\cis\\*.csv"
    //    val imd_file = "C:\\CEC_Documents\\Curated\\imd\\*.csv"
    //    val sce_qfer_path = "C:\\CEC_Documents\\Curated\\sce_qfer\\*.csv"
    //    val pge_qfer_path = "C:\\CEC_Documents\\Curated\\pge_qfer\\*.csv"
    //    val NAICS_path = "C:\\CEC_Documents\\Curated\\naics_*.csv"
    //    val ZIP_path = "C:\\CEC_Documents\\Curated\\ziptoiou*.csv"
    //    val CACounty_path = "C:\\CEC_Documents\\Curated\\county*.csv"

    //    var final_cis_imd_out = "/cec/purposebuilt/confpii/imd/final_cis_imd_out.csv"
    //    var final_qfer_out = "/cec/purposebuilt/confpii/imd/final_qfer_out.csv"
    //    var final_cis_imd_qfer_out = "/cec/purposebuilt/confpii/imd/final_cis_imd_qfer_out.csv" 
    //    val sce_qfer_path = "/cec/ead/curated/confnonpii/imd/sceqfer/sceqfer*.csv"
    //    val pge_qfer_path = "/cec/ead/curated/confnonpii/imd/pgeqfer/pgeqfer*.csv"


    //    var final_cis_imd_out = "C:\\CEC_Documents\\Curated\\Outputfiles\\final_cis_imd_out.csv"
    //    var final_qfer_out = "C:\\CEC_Documents\\Curated\\Outputfiles\\final_qfer_out.csv"
    //    var final_cis_imd_qfer_out = "C:\\CEC_Documents\\Curated\\Outputfiles\\final_cis_imd_qfer_out.csv"

    
    
    //val imd_file = ("/cec/ead/curated/confnonpii/imd/pgeresimd/*.csv,/cec/ead/curated/confnonpii/imd/scenonresimd/*.csv,/cec/ead/curated/confnonpii/imd/sceresimd/*.csv")
    //val cis_file = ("/cec/ead/curated/confpii/imd/pgerescis/*.csv,/cec/ead/curated/confpii/imd/scenonrescis/*.csv,/cec/ead/curated/confpii/imd/scerescis/*.csv")

    //System.setProperty("hadoop.home.dir", "C:\\SparkDev");
    //System.setProperty("spark.sql.warehouse.dir", "C:\\spark-warehouse");
    //val sparkconf = new SparkConf().setAppName("cec").setMaster("local")

    /**
     * Spark configuration setup with serializer
     */
    val sparkconf = new SparkConf().setAppName("cec")
    sparkconf.set("org.apache.spark.serializer.KryoSerializer", "spark_serializer")
    val sc = new SparkContext(sparkconf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    //Loading the CIS file into a dataframe
    val SCE_proxy_RES_CIS = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", "\t").load(cis_file)
    val zip = udf((t1: String) => if (t1 != null) t1.split("CA").last.trim().split("-")(0).trim() else "")

    //UDF for date conversion
    val format: SimpleDateFormat = new SimpleDateFormat("MM/dd/yy") //Input format
    val formatter: SimpleDateFormat = new SimpleDateFormat("yyMM") //Output format
    val yearmonth = udf((t2: String) => if (t2 != null) formatter.format(format.parse(t2)) else "")
    val new_Master_file = SCE_proxy_RES_CIS.withColumn("zip", zip(col("service_address"))).withColumnRenamed("naics", "naics_cis")
    val Extrapolated = udf((t3: String, t4: String) => if (t3 != null) if (t3.contains("Y")) t4 else "" else "")

    val SCE_proxy_RES_IMD_temp = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", "\t").option("delimiter", "\t").option("mode", "permissive").load(imd_file)
    val SCE_proxy_RES_IMD = SCE_proxy_RES_IMD_temp.filter("date != 'date'")
    val new_imd_file = SCE_proxy_RES_IMD.withColumn("Year_Month", yearmonth(col("date"))).withColumn("Extrapolated_value", Extrapolated(col("status"), col("kwh")))
    val Imd_collect = new_imd_file.groupBy("Account_ID", "Year_Month").agg(sum("kwh").as("sum_kwh"), sum("Extrapolated_value").as("sum_Extrapolated_value"))

    //Loading all the reference files here NAICS, ZIP & CAcounty
    val NAICS_Validation = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", "\t").option("mode", "permissive").load("/cec/ead/global/curated/nonconf/imd/naicsvalidation/naics*.csv")
    val ZIP = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", "\t").option("mode", "permissive").load("/cec/ead/global/curated/nonconf/referencedata/ziptoioumapping/ziptoioumapping*.csv")
    val CACounty = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", "\t").option("mode", "permissive").load("/cec/ead/global/curated/nonconf/referencedata/cacountycodes/countycodes*.csv")

    /*val NAICS_Validation = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", "\t").option("mode", "permissive").load(NAICS_path)
    val ZIP = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", "\t").option("mode", "permissive").load(ZIP_path)
    val CACounty = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", "\t").option("mode", "permissive").load(CACounty_path) */

    val CACounty1 = CACounty.withColumn("fips_county", substring(col("fips_county"), 1, 4))

    val joindf1 = new_Master_file.as("D1").join(NAICS_Validation.as("D2"), new_Master_file("naics_cis") === NAICS_Validation("naics"))

    val joindf2 = joindf1.join(ZIP, Seq("zip"))
    val final_cis = joindf2.join(CACounty1, joindf2("county") === CACounty1("county_name")).filter($"state" === "California")
    val cis_imd = final_cis.join(Imd_collect, final_cis("account_id") === Imd_collect("Account_ID"))

    val final_cis_imd = cis_imd.select("account_id", "naics_cis", "zip", "service_address", "sum_kwh", "fips_county", "naics_description", "major_sector", "naics_category", "electric_utility_service_area", "naics_ab1", "Year_Month", "sum_Extrapolated_value", "county")

    //Reading all the QFER files in Dataframe
    val sce_qfer = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", "\t").option("mode", "permissive").load(sce_qfer_path)
    val pge_qfer = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", "\t").option("mode", "permissive").load(pge_qfer_path)

    //  val sce_qfer = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", "\t").option("mode", "permissive").load("C:\\imd\\Temp\\sce_qfer_01112018.csv")  
    //  val pge_qfer = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", "\t").option("mode", "permissive").load("C:\\imd\\Temp\\pge_qfer_01112018.csv")

    val year1 = udf((t1: String) => if (t1 != null) t1.splitAt(2)._2 else "")
    val union_qfer = sce_qfer.unionAll(pge_qfer).withColumn("month", lpad(col("month"), 2, "0")).withColumn("year_part", year1(col("year")))

    val qfer_yearmonth = udf((t6: String, t7: String) => if ((t6 != null) && (t7 != null)) t6 + t7 else "")

    val total_qfer = union_qfer.withColumn("q_yearmonth", qfer_yearmonth(col("year_part"), col("month"))).select("q_yearmonth", "countyCode", "NAICS", "kwh")
    val final_qfer = total_qfer.withColumnRenamed("kwh", "qfer_kwh")

    val final_qfer1 = final_qfer.groupBy("NAICS", "countyCode", "q_yearmonth").agg(sum("qfer_kwh").as("sum_qfer_kwh"))

    val final_imd_qfer_cis = final_cis_imd.as("I").join(final_qfer1.as("Q"), final_cis_imd("Year_Month") === final_qfer1("q_yearmonth") && final_cis_imd("naics_cis") === final_qfer1("NAICS") && final_cis_imd("fips_county") === final_qfer1("countyCode"))
    //val final_imd_qf_cis = final_cis_imd.as("I").join(final_qfer1.as("Q"),final_cis_imd("Year_Month") === final_qfer1("q_yearmonth") && final_cis_imd("naics_cis") === final_qfer1("NAICS"))

    //val iqcis =  final_cis_imd.as("I").join(final_qfer1.as("Q"),final_cis_imd("naics_cis") === final_qfer1("NAICS") && final_cis_imd("fips_county") === final_qfer1("countyCode"))
    //val iqcis1 =  final_cis_imd.as("I").join(final_qfer1.as("Q"),final_cis_imd("fips_county") === final_qfer1("countyCode") && final_cis_imd("Year_Month") === final_qfer1("q_yearmonth"))
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = FileSystem.get(sc.hadoopConfiguration)
    
    hdfs.delete(new Path("/cec/ead/global/raw/temp/"), true)
    hdfs.delete(new Path(final_cis_imd_out), true)
    //Writng all the Output files in temp folder and then renaming the file and deleting the temp folder
    final_cis_imd.repartition(1).write.format("com.databricks.spark.csv").mode("append").option("delimiter", "\t").option("header", "true").save("/cec/ead/global/raw/temp")
    hdfs.rename(new Path("/cec/ead/global/raw/temp/part-00000"), new Path(final_cis_imd_out))
    hdfs.delete(new Path("/cec/ead/global/raw/temp/"), true)

    hdfs.delete(new Path(final_qfer_out), true)
    final_qfer1.repartition(1).write.format("com.databricks.spark.csv").mode("append").option("delimiter", "\t").option("header", "true").save("/cec/ead/global/raw/temp")
    hdfs.rename(new Path("/cec/ead/global/raw/temp/part-00000"), new Path(final_qfer_out))
    hdfs.delete(new Path("/cec/ead/global/raw/temp/"), true)

    hdfs.delete(new Path(final_cis_imd_qfer_out), true)
    final_imd_qfer_cis.repartition(1).write.format("com.databricks.spark.csv").mode("append").option("delimiter", "\t").option("header", "true").save("/cec/ead/global/raw/temp")
    hdfs.rename(new Path("/cec/ead/global/raw/temp/part-00000"), new Path(final_cis_imd_qfer_out))
    hdfs.delete(new Path("/cec/ead/global/raw/temp/"), true)

    //Metadata Files is been process and written here
    val schemaString = "Source,Destination,Size,Row_Count,Time"
    val fields = schemaString.split(",").map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    //val a = org.apache.hadoop.fs.FileSystem.get(new URI(cis_file), hadoopConf).getContentSummary(new Path(cis_file)).getSpaceConsumed
    //val arr = cis_file.split(",")
    //val pathSet = Set()
    //for(adl_path<-arr){
    //var listStatus = org.apache.hadoop.fs.FileSystem.get(new URI(adl_path), hadoopConf).globStatus(new org.apache.hadoop.fs.Path(EnrichedFile.replaceAll(adl_path, "") + "/*.csv"))
    //var path_01 = listStatus(0).getPath()
    //val path01 = path_01.toString()
    //pathSet += path01
    //}      

    val cis = List(Row("CIS paths :" + cis_file, final_cis_imd_out, "NIL", SCE_proxy_RES_CIS.count().toString(), PresentDate()), Row("IMD paths :" + imd_file, final_cis_imd_out, "NIL", SCE_proxy_RES_IMD.count().toString(), PresentDate()), Row("SCE QFER path :" + sce_qfer_path, final_cis_imd_out, "NIL", sce_qfer.count().toString(), PresentDate()), Row("PGE QFER path :" + pge_qfer_path, final_cis_imd_out, "NIL", pge_qfer.count().toString(), PresentDate()))
    val rdd = sc.parallelize(cis)
    val finalDF = sqlContext.createDataFrame(rdd, schema)

    //    hdfs.delete(new Path("C:\\CEC_Documents\\Curated\\temp\\"), true)
    //    finalDF.repartition(1).write.format("com.databricks.spark.csv").option("delimiter", "\t").option("header", "true").save("C:\\CEC_Documents\\Curated\\temp")
    //    hdfs.rename(new Path("C:\\CEC_Documents\\Curated\\temp\\part-00000"), new Path("C:\\CEC_Documents\\Curated\\Output\\Metadata.csv"))
    //    hdfs.delete(new Path("C:\\CEC_Documents\\Curated\\temp\\"), true)

    hdfs.delete(new Path("/cec/purposebuilt/confpii/imd/temp"), true)
    finalDF.repartition(1).write.format("com.databricks.spark.csv").option("delimiter", "\t").option("header", "true").save("/cec/purposebuilt/confpii/imd/temp")
    hdfs.rename(new Path("/cec/purposebuilt/confpii/imd/temp/part-00000"), new Path("/cec/purposebuilt/confpii/imd/metadata/metadata_curated.csv"))
    hdfs.delete(new Path("/cec/purposebuilt/confpii/imd/temp"), true)
  }

}