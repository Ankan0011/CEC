package com.cec.rawstage;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;

public class DateUtil {
	/**
	 * This method checks for missing timestamps and returns 3 previous timestamps associated with each record.
	 * @param dateTime
	 * @param fedList
	 */
	public static ArrayList<String> dateCalcalution(ArrayList<String> dateTime, ArrayList<String> fedList) throws ParseException
	{
		Calendar cal = Calendar.getInstance();
		SimpleDateFormat format = new SimpleDateFormat("MM/dd/yy HH:mm:ss");
		Date prevDate = null;
		String dates;
		String temp[];
		String tempVal;
		String concatenatedDate;
		ArrayList<String> returnList = new ArrayList<String>();
		
		System.out.println(System.currentTimeMillis() + " Calculating Previous Dates....!!");
		
		Iterator<String> itr = dateTime.iterator();
		while(itr.hasNext()){  
		tempVal = itr.next().toString();
		temp = tempVal.split(",");
		concatenatedDate = temp[1] + " " + temp[2];
		Date date = format.parse(concatenatedDate);		//date for which missing value is found.
		//System.out.println("Current Date : " + date);
		cal.setTime(date);
		int dayOfWeek = cal.get(Calendar.DAY_OF_WEEK);
		
				
		if(fedList.contains(temp[1])){		//If a missing value is on federal holiday.
			//System.out.println("Date found on Federal Holiday");
			for(int i=1; i<4; i++){
				prevDate = DateUtil.getWeekendPrevDate(cal);
				dates = format.format(prevDate);
			//	System.out.println(i + " prevDate : " + dates);
				dates = checkIntermediateFederal(dates,concatenatedDate,fedList);
				returnList.add(tempVal + " " + dates);
			}
		} else if(dayOfWeek == 1 || dayOfWeek == 7){		//If a missing value is on weekend.
			//System.out.println("Date found on weekend");
			for(int i=1; i<4; i++){
				prevDate = DateUtil.getWeekendPrevDate(cal);
				dates = format.format(prevDate);
				//System.out.println(i + " prevDate : " + dates);
				returnList.add(tempVal + " " + dates);
			}
			
		} else {					//If a missing value is on weekdays.
			//System.out.println("Date found on week days");
			
			for(int i=1; i<4; i++){
				prevDate = DateUtil.getPrevDate(cal);
				dates = format.format(prevDate);
					if(fedList.contains(dates.split(" ")[0])){	//skip federal holidays coming on weekdays
						prevDate = DateUtil.getPrevDate(cal);	
					}
					dates = format.format(prevDate);
					//System.out.println(i + " prevDate : " + dates);
					returnList.add(tempVal + " " + dates);
			}
		}
		}
		
		System.out.println(System.currentTimeMillis() + " Previous Dates Calculated.....!!!!");
		return returnList;
	}

	 /**
	 * method is getting previous dates when missing value is on weekdays.
	 * @param cal
	 */
		private static Date getPrevDate(Calendar cal){
			cal.add(Calendar.DAY_OF_YEAR, -1);
			int dayOfWeek = cal.get(Calendar.DAY_OF_WEEK);
			if(dayOfWeek == 1 || dayOfWeek == 7){
				cal.add(Calendar.DAY_OF_YEAR, -((dayOfWeek + 1) %7));
			}
			Date date = cal.getTime();
			return date;
		}

		
	/**
	 * method is getting previous dates when missing value is on weekends.
	 * @param cal
	 */
		private static Date getWeekendPrevDate(Calendar cal){
			cal.add(Calendar.DAY_OF_YEAR, -1);
			int dayOfWeek = cal.get(Calendar.DAY_OF_WEEK);
			if(dayOfWeek != 1 && dayOfWeek != 7){
			cal.add(Calendar.DAY_OF_YEAR, -((dayOfWeek + 6) %7));	
			} else if (dayOfWeek == 7){
			cal.add(Calendar.DAY_OF_YEAR, -((dayOfWeek + 7) %7));
			}
			Date date = cal.getTime();
			return date;
		}
		
	/**
	 * method is checking if there is any federal holiday in between three weekend dates calculated.
	 * @param weekendBeforeFederal
	 * @param federalDate
	 * @param fedList
	 */	
		private static String checkIntermediateFederal(String weekendBeforeFederal, String federalDate, ArrayList<String> fedList) {
			
			Calendar cal = Calendar.getInstance();
			SimpleDateFormat format = new SimpleDateFormat("MM/dd/yy HH:mm:ss");
			String intermediateDate = null;
			String intermediateFedDate = null;
			String federalDateSplitter[] = federalDate.split(" ");
			String intermediateDateSplitter[]=null;
			
			try {
				Date date = format.parse(weekendBeforeFederal);
				cal.setTime(date);
				intermediateDate = format.format(cal.getTime());
								
				while(!intermediateDate.equals(federalDate)){
					cal.add(Calendar.DAY_OF_YEAR, 1);
					intermediateDate = format.format(cal.getTime());
					intermediateDateSplitter = intermediateDate.split(" ");
					if(fedList.contains(intermediateDateSplitter[0]) && !intermediateDateSplitter[0].equals(federalDateSplitter[0])){
						intermediateFedDate = intermediateDate;
						break;
					}else {
						intermediateFedDate = weekendBeforeFederal;
					}
				}
				
			} catch (ParseException e) {
				e.printStackTrace();
			}
			
			return intermediateFedDate;
		}
		
	/*	public static void main(String[] args) {
		
		ArrayList<String> fedList = new ArrayList<String>();
		fedList.add("12/25/17");
		fedList.add("12/20/17");
		ArrayList<String> missingList = new ArrayList<String>();
		missingList.add("accountId,12/25/17,00:15:00");
		ArrayList<String> outputList = new ArrayList<String>();
		try {
			outputList = dateCalcalution(missingList, fedList);
			System.out.println(outputList);
			
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
	}
*/
}
