package com.cec.rawstage;

import static org.apache.spark.sql.functions.lit;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SQLContext;

public class calculateAverage {
	
	/**
	 * method calculates average kwh value from previous dates calculated from missing records.
	 * @param missingDateList
	 * @param rawDataList
	 * @param column
	 * @param fedlist
	 * @param sqlContext
	 * @param sc
	 * @param finalProcess
	 * @return
	 * @throws ParseException
	 * @throws IOException
	 */	
	public static boolean calFinalIMDAvg(ArrayList<String> missingDateList,ArrayList<String> rawDataList,String column, ArrayList<String> fedlist, SQLContext sqlContext,SparkContext sc,String finalProcess) throws ParseException, IOException{
		
		boolean flag = false;
			if (column.contains("kwh")){
			flag = true;
			//DataFrame missingDateDF = sqlContext.sql("select Account_ID,date,time from rawtable where kwh = ''");	//checks and stores blank records.
			//ArrayList<String> missingDateList = CreateDF.toArrayList(missingDateDF);
			
			//missingDateDF.show();
			
			DataFrame newDF;
			ArrayList<String> prevRecords = DateUtil.dateCalcalution(missingDateList, fedlist); //function to get previous dates.
			//System.out.println("Previous Dates Records : " + prevRecords.size());
			//System.out.println("sdashasjkhdasjdhjashdasjdhasj"+ missingDateList.size());
			ArrayList<String> missingRecords = new ArrayList<String>();
			
			String Account_ID;
			String date;
			String time;
			String kwh;
			String peak_kwh;
			String status;
			 
			 ArrayList<String> averageList = new ArrayList<String>();
			 double average = 0.0;
			 String prevDate1;
			 String prevDate2;
			 String prevDate3;
			 String timeStamp;
			 String tempAcc;
			// prevRecords.parallelStream().filter(s -> s);
			 for(int i = 0; i < prevRecords.size(); i++){
				 prevDate1 = prevRecords.get(i).split(" ")[1];	// first previous date e.g. 12/29/17
				 if(!prevRecords.get(++i).isEmpty()){
				 prevDate2 = prevRecords.get(i).split(" ")[1];	// second previous date e.g. 12/28/17
				 }else{
					 prevDate2 = prevDate1; 
				 }
				 if(!prevRecords.get(++i).isEmpty()){
					 prevDate3 = prevRecords.get(i).split(" ")[1];	// third previous date e.g. 12/27/17
				 }else{
					 prevDate3 = prevDate2;
				 }
				 timeStamp = prevRecords.get(i).split(" ")[2];
				 tempAcc = prevRecords.get(i).split(",")[0];
				newDF = sqlContext.sql("select kwh from rawtable where (date='"+prevDate1+"' OR date='"+prevDate2+"' OR date='"+prevDate3+"') AND time='"+timeStamp+"' AND Account_ID='"+tempAcc+"'");
				//newDF.show();
				averageList = CreateDF.toArrayList(newDF);

				if(!averageList.isEmpty()){
					
					if(!averageList.get(0).isEmpty() && !averageList.get(1).isEmpty() && !averageList.get(2).isEmpty() ){
						average = (Double.parseDouble(averageList.get(0)) + Double.parseDouble(averageList.get(1)) + Double.parseDouble(averageList.get(2))) / 3;
					}else if(!averageList.get(0).isEmpty() && !averageList.get(1).isEmpty()){
						average = (Double.parseDouble(averageList.get(0)) + Double.parseDouble(averageList.get(1))) / 2;
					}else if(!averageList.get(0).isEmpty()){
						average = (Double.parseDouble(averageList.get(0))/ 1);
					}
								
				}
				
				Account_ID = prevRecords.get(i).split(",")[0];
				date = prevRecords.get(i).split(",")[1];
				time = prevRecords.get(i).split(",")[2].split(" ")[0];
				kwh = Double.toString(average);
				peak_kwh = "0.0";
				status = "Y";
				missingRecords.add(Account_ID+","+date+","+time+","+kwh+","+peak_kwh+","+status);
			 }
			 
			 DataFrame missingDF = createMissingDF(missingRecords,  sqlContext);
			 
			 missingDF = missingDF.distinct();
		
			 DataFrame validDF = sqlContext.sql("select * from rawtable");
			 DataFrame newDF1 = validDF.withColumn("status", lit("N"));
			 
			  DataFrame FinalDF = newDF1.unionAll(missingDF);	//merging of two dataframes having missing and valid values 
			  
			  DateFormat dateFormat = new SimpleDateFormat("yyMMddHHmmss");
			    Date datetime = new Date();
		
			     FileSystem fs = FileSystem.get(sc.hadoopConfiguration());
				 fs.delete(new Path("/cec/ead/global/raw/temp/"), true);
				 fs.delete(new Path(finalProcess), true);
				 FinalDF.repartition(1).write().format("com.databricks.spark.csv").option("delimiter", "\t").option("header", "true").save("/cec/ead/global/raw/temp");
				
				 fs.rename(new Path("/cec/ead/global/raw/temp/part-00000"), new Path(finalProcess));
				 fs.delete(new Path("/cec/ead/global/raw/temp/"), true);
				  
				/*FileSystem fs = FileSystem.get(sc.hadoopConfiguration());
				fs.delete(new Path("C:\\cec\\processed_imd_temp\\"), true);
			    FinalDF.repartition(1).write().format("com.databricks.spark.csv").option("delimiter", "\t").option("header", "true").save("C:\\cec\\processed_imd_temp");
			       
			    fs.rename(new Path("C:\\cec\\processed_imd_temp\\part-00000"), new Path(finalProcess));
			    fs.delete(new Path("C:\\cec\\processed_imd_temp\\"), true);*/

			 }		
		
		
		return flag;
		
	}
	
	/**
	 * Method creates Dataframe for missing records with specified schema.
	 * @param missingRecords
	 * @param sqlContext
	 * @return
	 */	
	private static DataFrame createMissingDF(ArrayList<String> missingRecords, SQLContext sqlContext)
	{
		 DataFrame df = sqlContext.createDataset(missingRecords, Encoders.STRING()).toDF();
		DataFrame missingDF = df.selectExpr("split(value, ',')[0] as Account_ID", 
				 "split(value, ',')[1] as date", 
				 "split(value, ',')[2] as time", 
				 "split(value, ',')[3] as kwh", 
				 "split(value, ',')[4] as peak_kwh", 
				 "split(value, ',')[5] as status");
		return missingDF;
		
	}
	
	/**
	 * Method creates Dataframe for metadata raw file with specified schema.
	 * @param missingRecords
	 * @param sqlContext
	 * @return
	 */	
	public static DataFrame createMetaFileRaw(ArrayList<String> metafilerawfinal, SQLContext sqlContext)
	{
		DataFrame rawMetafiledf = sqlContext.createDataset(metafilerawfinal, Encoders.STRING()).toDF();
		DataFrame rawMetadatadf = rawMetafiledf.selectExpr("split(value, ',')[0] as Source", 
				 "split(value, ',')[1] as Distination", 
				 "split(value, ',')[2] as File_Size", 
				 "split(value, ',')[3] as Row_Count", 
				 "split(value, ',')[4] as Time" 
				 );
		return rawMetadatadf;
		
	}
}
