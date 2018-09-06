package com.cec.rawstage;

import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class metadata_merge {

	public static void main(String[] args) throws IllegalArgumentException, IOException {
		String rawfile = "/cec/ead/raw/confnonpii/imd/metadata/*.csv";
		String processedFile = "/cec/ead/raw/confnonpii/imd/metadata_curated/metastore_curated.csv";
		
		//String rawfile = "C:\\CEC_Documents\\Metadata\\*.csv";
		//String processedFile = "C:\\CEC_Documents\\Merged\\metastore_curated.csv";
		//rawfile = "/user/sonus/pge_res_cis_01112018.csv";
		//processedFile = "/user/sonus/cis_merge/cis.csv";
		
//		if(!args[0].equals("")&& !args[0].equals(null) && !args[1].equals("")&& !args[1].equals(null))
//		{
//			rawfile = args[0];
//			processedFile = args[1];
//		}
//		else
//		{
//			System.out.println("Please provide input and output file path");
//			System.exit(0);
//		} 
		//System.setProperty("hadoop.home.dir", "C:\\SparkDev");
		//System.setProperty("spark.sql.warehouse.dir", "C:\\spark-warehouse");
	    SparkConf conf = (new SparkConf()).setAppName("Metadata_Merge");
	    conf.set("org.apache.spark.serializer.KryoSerializer", "spark_serializer"); 
 


SparkContext sc = new SparkContext(conf);
SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);
		
DataFrame cis_df = CreateDF.createDF(rawfile,",", sqlContext);
FileSystem fs = FileSystem.get(sc.hadoopConfiguration());
fs.delete(new Path("/cec/ead/raw/confnonpii/imd/metadata_temp/"), true);
cis_df.repartition(1).write().format("com.databricks.spark.csv").option("delimiter", ",").option("header", "true").save("/cec/ead/raw/confnonpii/imd/metadata_temp");

fs.rename(new Path("/cec/ead/raw/confnonpii/imd/metadata_temp/part-00000"), new Path(processedFile));
fs.delete(new Path("/cec/ead/raw/confnonpii/imd/metadata_temp/"), true);

/*fs.delete(new Path("C:\\CEC_Documents\\temp\\"), true);
cis_df.repartition(1).write().format("com.databricks.spark.csv").option("delimiter", ",").option("header", "true").save("C:\\CEC_Documents\\temp");

fs.rename(new Path("C:\\CEC_Documents\\temp\\part-00000"), new Path(processedFile));
fs.delete(new Path("C:\\CEC_Documents\\temp\\"), true);*/
	}

}
