package com.cec.rawstage;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class IMDResources {
	
	public static final SimpleDateFormat SDF = new SimpleDateFormat("MM/dd/yy HH:mm:ss");
	public static final SimpleDateFormat dateSDF = new SimpleDateFormat("MM/dd/yy");
	public static final SimpleDateFormat timeSDF = new SimpleDateFormat("HH:mm:ss");
	
	
	private String accountId;
	private Date datetime;
	private Double kwh;
	private Double peakDemand;
	
	public IMDResources(String accountId, String date, String time, String kwh, String peakDemand){
		setAccountId(accountId);
		setDatetime(date, time);
		setKwh(kwh);
		setPeakDemand(peakDemand);
	}
	
	public IMDResources(String accountId, Date datetime, String kwh, String peakDemand){
		setAccountId(accountId);
		setDatetime(datetime);
		setKwh(kwh);
		setPeakDemand(peakDemand);
	}
	
	/**
	 * @return the accountId
	 */
	public String getAccountId() {
		return accountId;
	}
	/**
	 * @param accountId the accountId to set
	 */
	public void setAccountId(String accountId) {
		this.accountId = accountId;
	}
	
	/**
	 * @return the datetime
	 */
	
	public Date getDatetime() {
		return datetime;
	}
	
	public String getDate(){
		
		return dateSDF.format(this.datetime); 
	}
	
	public String getTime(){
		
		return timeSDF.format(this.datetime); 
	}
	
	public String getDateTimeString(){
		
		return SDF.format(this.datetime); 
	}
	
	/**
	 * @param datetime the datetime to set
	 */
	public void setDatetime(String date, String time) {
		try{
			SDF.setTimeZone(TimeZone.getTimeZone("UTC"));
			this.datetime = SDF.parse(date + " " + time);
		}catch(ParseException  e){
			this.datetime = null;
		}
	}
	
	/**
	 * @param datetime the datetime to set
	 */
	public void setDatetime(Date datetime) {
		this.datetime = datetime;
	}
	/**
	 * @return the kwh
	 */
	public Double getKwh() {
		return kwh;
	}
	/**
	 * @param kwh the kwh to set
	 */
	public void setKwh(String kwh) {
		try{
			this.kwh = Double.valueOf(kwh);
		}catch(Exception e){
			this.kwh = null;
		}
	}
	/**
	 * @return the peakDemand
	 */
	public Double getPeakDemand() {
		return peakDemand;
	}
	/**
	 * @param peakDemand the peakDemand to set
	 */
	public void setPeakDemand(String peakDemand) {
		try{
			this.peakDemand = Double.valueOf(peakDemand);
		}catch(Exception e){
			this.peakDemand = null;
		}
	}
	
	/**
	 * This method is used to format data in String so that it can be easily debugged
	 */
	@Override
	public String toString(){
		String kwh = "";
		String peakDemand = "";
		if(this.kwh==null){
			kwh="";
		}else{
			kwh=this.kwh.toString();
		}
		if(this.peakDemand==null){
			peakDemand="";
		}else{
			peakDemand=this.peakDemand.toString();
		}
		
		String records = accountId+","+getDate()+","+getTime()+","+kwh+","+peakDemand;
		return records;
	}
	
	/**
	 * This method will return true or false depending upon the validation applied
	 * @return
	 */
	public boolean isValid(){
		if((accountId == null) || (datetime == null))
			return false;
		else
			return true;
	}
}