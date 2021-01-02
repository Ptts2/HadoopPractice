//MaxMinTemperatureReducer
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class MaxMinTemperatureReducer
  extends Reducer<Text, TempAndQuality, Text, TempAndQuality> {

  @Override
  public void reduce(Text key, Iterable<TempAndQuality> values,
      Context context)
      throws IOException, InterruptedException {
    
    int maxValueQ1 = Integer.MIN_VALUE;
    int minValueQ1 = Integer.MAX_VALUE;
    
    int maxValueGQ = Integer.MIN_VALUE;
    int minValueGQ = Integer.MAX_VALUE;
    
    int maxValueNQ = Integer.MIN_VALUE;
    int minValueNQ = Integer.MAX_VALUE;
    
    for (TempAndQuality value : values) {
      
      String quality = value.getQuality().toString();
      String tmp = value.getAirTemperature().toString();
       
      if(isNumeric(quality) && isNumeric(tmp)){
      	int temp = Integer.parseInt(tmp);
      
      	if(quality.equals("1")){
      		maxValueQ1 = Math.max(maxValueQ1, temp);
      		minValueQ1 = Math.min(minValueQ1, temp);
      	}
      
      	if( quality.matches("[01459]") ){
      		maxValueGQ = Math.max(maxValueGQ, temp);
      		minValueGQ = Math.min(minValueGQ, temp);
      	}
      
      	maxValueNQ = Math.max(maxValueNQ, temp);
      	minValueNQ = Math.min(minValueNQ, temp);
      }
      
    }
    
    
    context.write(key, new TempAndQuality(maxValueQ1, minValueQ1, maxValueGQ, minValueGQ, maxValueNQ, minValueNQ));
  }
  
  public static boolean isNumeric(String str)  {  
  	try  {  
    	int n = Integer.parseInt(str);  
  	}  
  	catch(NumberFormatException e)  {  
    		return false;  
  	}	  
  	return true;  
  }
}

