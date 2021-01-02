//NcdcRecordParser A class for parsing weather records in NCDC format
import org.apache.hadoop.io.Text;

public class NcdcRecordParser {
  
  private static final int MISSING_TEMPERATURE = 9999;
  private static final int LOWEST_TEMP = -100;
  private static final int HIGHEST_TEMP = 100;
  
  private String year;
  private int airTemperature;
  private String quality;
  
  public void parse(String record) {
    year = record.substring(15, 19);
    String airTemperatureString;
    // Remove leading plus sign as parseInt doesn't like them (pre-Java 7)
    if (record.charAt(87) == '+') { 
      airTemperatureString = record.substring(88, 92);
    } else {
      airTemperatureString = record.substring(87, 92);
    }
    airTemperature = Integer.parseInt(airTemperatureString);
    quality = record.substring(92, 93);
  }
  
  public void parse(Text record) {
    parse(record.toString());
  }

  public boolean isValidTemperature() {
    return (airTemperature != MISSING_TEMPERATURE) && ((airTemperature/10) > LOWEST_TEMP) &&  ((airTemperature/10) < HIGHEST_TEMP);
  }
  
  public String getYear() {
    return year;
  }
  
  public String getQuality(){
  	return this.quality;
  }
  
  public double getRealAirTemperature(){
  	return this.airTemperature/10;
  }
  
  public int getAirTemperature() {
    return airTemperature;
  }
}

