package com.cec.rawstage;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;

public class TabDelimiter implements Function<Row, String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public String call(Row arg0) throws Exception {
		return arg0.mkString("\t");
	}

}