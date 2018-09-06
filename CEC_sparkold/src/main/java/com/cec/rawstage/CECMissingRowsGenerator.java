package com.cec.rawstage;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class CECMissingRowsGenerator {
	
	/**
	 * This method iterates from raw data List and generates data for missing rows in List
	 * @param rawData
	 * @return
	 */
	public ArrayList<String> generateMissingRows(ArrayList<String> rawData){
		String fileDelimiter = ",";
		Map<String, Date> masterMap = new HashMap<String, Date>();
		ArrayList<String> missingList = new ArrayList<String>();
		CECMissingRowsGenerator obj = new CECMissingRowsGenerator();
		try{
			for(String str : rawData){
			String[] columns = str.split(fileDelimiter);
			IMDResources imd = new IMDResources(columns[0], columns[1], columns[2], columns[3], columns[4]);
			obj.generateMissingRow(imd, masterMap, missingList);
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		return missingList;
	}

	/**
	 * This method checks for current record with latest record present in Map
	 * @param imd
	 * @param masterMap
	 * @param missingList
	 */
	private void generateMissingRow(IMDResources imd, Map<String, Date> masterMap, List<String> missingList){
		CECMissingRowsGenerator obj = new CECMissingRowsGenerator();
		if(masterMap.containsKey(imd.getAccountId())){
			if(obj.isRecordMissing(masterMap.get(imd.getAccountId()), imd.getDatetime(), 20L)){
				while(obj.isRecordMissing(masterMap.get(imd.getAccountId()), imd.getDatetime(), 20L)){
					IMDResources imd2 = new IMDResources(imd.getAccountId(), obj.addMinutes(masterMap.get(imd.getAccountId()), 15), null, null);
					masterMap.put(imd2.getAccountId(), imd2.getDatetime());
					if(imd2.isValid())
						missingList.add(imd2.toString());//Add to File
				}
				masterMap.put(imd.getAccountId(), imd.getDatetime());
				//outList.add(imd.toString());//Add to File
			}else{
				masterMap.put(imd.getAccountId(), imd.getDatetime());
				//outList.add(imd.toString());//Add to File
			}
		}else{
			masterMap.put(imd.getAccountId(), imd.getDatetime());
			//outList.add(imd.toString());//Add to File
		}
	}
	
	/**
	 * This method adds (time) minutes in passed Date 
	 * @param date
	 * @param time
	 * @return
	 */
	private Date addMinutes(Date date, int time){
        Date newDate = new Date(date.getTime() + (time * 60 * 1000));
        return newDate;
	}

	/**
     * date1 is old date, date2 is new date, timeUnit is desired timeunit of output 
      * @param date1
     * @param date2
     * @param timeUnit
     * @return
     * @throws ParseException 
      */
	private boolean isRecordMissing(Date date1, Date date2, Long timeDiff) {
		TimeUnit timeUnit = TimeUnit.MINUTES;
		long calculated = timeUnit.convert((date2.getTime() - date1.getTime()), TimeUnit.MILLISECONDS);
         
		boolean flag = false;
		if(calculated >= timeDiff)
			flag = true;
		else 
			flag = false;
		return flag;
	}
}