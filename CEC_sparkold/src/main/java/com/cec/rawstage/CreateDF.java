package com.cec.rawstage;

import java.util.ArrayList;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class CreateDF {
	
	/**
	 * Method creates Dataframe.
	 * @param filePath
	 * @param delimiter
	 * @param sqlContext
	 * @return
	 */	
	public static DataFrame createDF(String filePath, String delimiter, SQLContext sqlContext)
	{
		DataFrame df = sqlContext.read().format("com.databricks.spark.csv").option("header", "true").option("delimiter",delimiter).option("mode", "permissive").load(filePath );
		return df;
		
	}
	
	
	/**
	 * Method creates Dataframe for missing records with specified schema.
	 * @param missingRecords
	 * @param sqlContext
	 * @return
	 */	
	public static DataFrame createMissingDF(ArrayList<String> rawMissingList, SQLContext sqlContext)
	{
		DataFrame rawMissingdf = sqlContext.createDataset(rawMissingList, Encoders.STRING()).toDF();
		rawMissingdf = rawMissingdf.selectExpr("split(value, ',')[0] as Account_ID", 
				 "split(value, ',')[1] as date", 
				 "split(value, ',')[2] as time", 
				 "split(value, ',')[3] as kwh", 
				 "split(value, ',')[4] as peak_kwh");
		return rawMissingdf;
		
	}
	
	
	/**
	 * Method converts Dataframe to string arraylist.
	 * @param dataFrame
	 * @return
	 */	
	public static ArrayList<String> toArrayList(DataFrame dataFrame){
		ArrayList<String> list = new ArrayList<String>();
		 Row[] rows = dataFrame.collect();
		 for (Row row : rows) {
			 list.add(row.toString().substring(1, row.toString().length()-1));
			}
		return list;
		
	}

}
