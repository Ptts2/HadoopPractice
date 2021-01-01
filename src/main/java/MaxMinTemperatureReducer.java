//MaxTemperatureReducer Reducer for maximum temperature example
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxMinTemperatureReducer
  extends Reducer<Text, IntWritable, Text, IntWritable> {

  @Override
  public void reduce(Text key, Iterable<IntWritable> values,
      Context context)
      throws IOException, InterruptedException {
    
    int maxValueQ1 = Integer.MIN_VALUE;
    int minValueQ1 = Integer.MAX_VALUE;
    
    int maxValueGQ = Integer.MIN_VALUE;
    int minValueGQ = Integer.MAX_VALUE;
    
    int maxValueNQ = Integer.MIN_VALUE;
    int maxValueNQ = Integer.MAX_VALUE;
    
    String quality = key.toString.substring(30);
    String year = key.toString.substring(0,4);
    for (IntWritable value : values) {
      
      if(quality.matches(1)){
      	maxValueQ1 = Math.max(maxValueQ1, value.get());
      	minValueQ1 = Math.min(minValueQ1, value.get());
      }
      
      if(quality.matches([01459])){
      	maxValueGQ = Math.max(maxValueGQ, value.get());
      	minValueGQ = Math.min(minValueGQ, value.get());
      }
      
      maxValueNQ = Math.max(maxValueNQ, value.get());
      minValueNQ = Math.min(minValueNQ, value.get());
      
    }
    
    //Join all the temperatures in a string
    
    String solution = "Max Temperature with quality 1: "+String.valueOf(maxValueQ1)+"\nMin Temperature with quality 1: "+String.valueOf(minValueQ1)+"\nMax Temperature with quality in [01459]: "+String.valueOf(maxValueGQ)+"\nMin Temperature with quality in [01459]: "+String.valueOf(minValueGQ)+"\nMax Temperature without having quality into account: "+String.valueOf(maxValueNQ)+"\nMin Temperature without having quality into account: "+String.valueOf(minValueNQ);
    
    context.write(new Text(year), new Text(solution));
  }
}

