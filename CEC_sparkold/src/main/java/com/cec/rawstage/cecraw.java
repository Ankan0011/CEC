package com.cec.rawstage;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class cecraw {

	/**
	 * Method creates present system date in yyyy-MM-dd HH:mm:ss.SS for metadata
	 * raw file.
	 * 
	 * @return String
	 */
	public static String presentDate() {
		Date date = new Date();
		SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy h:mm:ss a");
		String formattedDate = sdf.format(date);
		return formattedDate;
	}
	/**
	 * Method creates an arraylist of all the path having csv file in raw file.
	 * @argument: Filesystem variable
	 * @argument: FileStatus array
	 * @return ArrayList<String>
	 */
	private static ArrayList<String> browse(FileSystem fs, FileStatus[] status)
			throws FileNotFoundException, IOException {

		ArrayList<String> results = new ArrayList<String>();
		FileStatus[] subStatus = null;

		for (int i = 0; i < status.length; i++) {
			FileStatus fileStatus = status[i];
			if (fileStatus.isDirectory()) {
				subStatus = fs.listStatus(fileStatus.getPath());

				browse(fs, subStatus);
				for (FileStatus subStatuss : subStatus) {
					boolean s = subStatuss.getPath().toString().endsWith(".csv");
					if (s == true) {
						results.add(subStatuss.getPath().toString());
					}

				}
			} else {

				String a = fileStatus.getPath().toString();
				boolean s = a.endsWith(".csv");
				if (s == true) {
					results.add(fileStatus.getPath().toString());
				}

			}
		}
		return results;
	}
	
	/**
	 * Main method for running raw to curated processing.
	 */

	public static void main(String[] args) throws Exception {
//		 String rawfile = null;
//		 String processedFile = " ";
//		 String federal_holidays = "src/main/resources/federal_holidays.txt";
//		 String Tempfederaltable = "federaltable1";
//		 String TempRawtable = "rawtable";

		String rawfile = " ";
		String processedFile = " ";
		String federal_holidays = "/user/sonus/federal_holidays.txt";
		String Tempfederaltable = "federaltable1";
		String TempRawtable = "rawtable";

		String input = " ";

		if (args[0].equals(null) || args[0].equals(" ") || args[1].equals(null) || args[1].equals(" ")) {
			System.exit(0);
		} else {
			input = args[0];
			processedFile = args[1];

		}

		 //System.setProperty("hadoop.home.dir", "C:\\SparkDev");
		 //System.setProperty("spark.sql.warehouse.dir", "C:\\spark-warehouse");
//		 SparkConf conf = (new
//		 SparkConf()).setMaster("local").setAppName("cec");
//		 conf.set("org.apache.spark.serializer.KryoSerializer",
//		 "spark_serializer");

		/**
	     * Spark configuration setup with serializer
	     */
		SparkConf conf = (new SparkConf()).setAppName("cecraw");
		conf.set("org.apache.spark.serializer.KryoSerializer", "spark_serializer");

		SparkContext sc = new SparkContext(conf);
		SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);
		FileSystem fs = null;
		FileStatus[] status = null;
		ArrayList<String> results = null;
		ArrayList<String> metafilerawfinal = new ArrayList<String>();
		String Source;
		String Destination;
		String Size;
		String Row_Count;
		String Date;

		// ArrayList<String> metafilecurated = new ArrayList<String>();
		try {
			fs = FileSystem.get(sc.hadoopConfiguration());
			/* status = fs.listStatus(new Path("C:\\cec\\raw_imd\\")); */
			status = fs.listStatus(new Path(input));
			results = browse(fs, status);

		} catch (IOException e) {
			e.printStackTrace();
		}

		for (String result : results) {
			rawfile = result;
			int i = rawfile.lastIndexOf("/");
			String OutputfileName = rawfile.substring(i + 1);

			System.out.println(rawfile);

			String finalProcess = processedFile + OutputfileName;

			Path filenamePath = new Path(rawfile);

			DataFrame fedDF = CreateDF.createDF(federal_holidays, ",", sqlContext);
			fedDF.registerTempTable(Tempfederaltable);

			fedDF = sqlContext.sql("select date from " + Tempfederaltable);

			DataFrame rawfileDF = CreateDF.createDF(rawfile, "\t", sqlContext);
			rawfileDF.registerTempTable(TempRawtable);
			rawfileDF.persist();

			ArrayList<String> fedlist = CreateDF.toArrayList(fedDF);
			ArrayList<String> rawDataList = CreateDF.toArrayList(rawfileDF);

			Source = rawfile;
			Destination = finalProcess;
			Size = String.valueOf(fs.getContentSummary(filenamePath).getSpaceConsumed()) + " bytes";
			Row_Count = Integer.toString(rawDataList.size());
			Date = presentDate();
			metafilerawfinal.add(Source + "," + Destination + "," + Size + "," + Row_Count + "," + Date);

			String[] columns = rawfileDF.schema().fieldNames(); // get column
																// names as
																// string array

			CECMissingRowsGenerator obj = new CECMissingRowsGenerator();
			ArrayList<String> rawMissingList = obj.generateMissingRows(rawDataList);

			// DataFrame rawMissingdf = CreateDF.createMissingDF(rawMissingList,
			// sqlContext);

			// rawfileDF = rawfileDF.unionAll(rawMissingdf);

			try {

				String colName = null;
				for (String s : columns) {
					if (s.contains("kwh")) {
						colName = s;
						break;
					}

				}

				calculateAverage.calFinalIMDAvg(rawMissingList, rawDataList, colName, fedlist, sqlContext, sc,
						finalProcess);
			} catch (Exception e) {
				e.printStackTrace();
			}
			fedDF.unpersist();
			rawfileDF.unpersist();
		}
		DataFrame rawMetafiledf = calculateAverage.createMetaFileRaw(metafilerawfinal, sqlContext);
		
		//Writing Metadata file @ location '/cec/ead/raw/confnonpii/imd/metadata'
		fs.delete(new Path("/cec/ead/raw/confnonpii/imd/temp"), true);
		rawMetafiledf.repartition(1).write().format("com.databricks.spark.csv").option("delimiter", "\t")
				.option("header", "true").save("/cec/ead/raw/confnonpii/imd/temp");
		fs.rename(new Path("/cec/ead/raw/confnonpii/imd/temp/part-00000"),
				new Path("/cec/ead/raw/confnonpii/imd/metadata/metadata_Raw_"+System.currentTimeMillis()+".csv"));
		fs.delete(new Path("/cec/ead/raw/confnonpii/imd/temp"), true);

		
//		  fs.delete(new Path("C:\\CEC_Documents\\temp"), true);
//		  rawMetafiledf.repartition(1).write().format("com.databricks.spark.csv").option("delimiter", "\t").option("header", "true").save("C:\\CEC_Documents\\temp");
//		  fs.rename(new Path("C:\\CEC_Documents\\temp\\part-00000"), new
//		  Path("C:\\CEC_Documents\\Metadata\\metaDataRaw_"+System.currentTimeMillis()+".csv")); 
//		  fs.delete(new Path("C:\\CEC_Documents\\temp"), true);
		 
	}
}