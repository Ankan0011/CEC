package com.cec.rawstage;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;

public class HelloWorld{
    public static void main (String [] args) throws Exception{
        
            FileSystem fs = FileSystem.get(new Configuration());
            FileStatus[] status = fs.listStatus(new Path("C:\\cec\\curated\\"));  // you need to pass in your hdfs path
            for (int i = 0; i < status.length; i++) {
                FileStatus fileStatus = status[i];
                if (!fileStatus.isDirectory()) {
                	fileStatus.getPath().toString();
                } 
                
               System.out.println("aaaaaaaaaaa" + fileStatus.getPath() );
            }
           
    }
}

