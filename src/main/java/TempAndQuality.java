import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

//Custom class for having all the temperatures
public class TempAndQuality implements WritableComparable<TempAndQuality> {

	
	//Mapper part
	private Text airTemperature;
	private Text quality;
	
	
	//Reducer part
	private Text maxTempQ1;
	private Text minTempQ1;
	
	private Text maxTempGQ;
	private Text minTempGQ;

	private Text maxTempNQ;
	private Text minTempNQ;
	
	
	
	public TempAndQuality(){
	
		this.airTemperature = new Text();
		this.quality = new Text();
		
		this.maxTempQ1 = new Text();
		this.minTempQ1 = new Text();
	
		this.maxTempGQ = new Text();
		this.minTempGQ = new Text();
	
		this.maxTempNQ = new Text();
		this.minTempNQ = new Text();
	
	}
	
	public TempAndQuality(int airTemp, String qlty){
	
		this.airTemperature = new Text(String.valueOf(airTemp));
		this.quality = new Text(qlty);
		
		this.maxTempQ1 = new Text();
		this.minTempQ1 = new Text();
	
		this.maxTempGQ = new Text();
		this.minTempGQ = new Text();
	
		this.maxTempNQ = new Text();
		this.minTempNQ = new Text();
	
	}
	
	
	public TempAndQuality(int maxTQ1, int minTQ1, int maxTGQ, int minTGQ, int maxTNQ, int minTNQ){
	
		this.maxTempQ1 = new Text(String.valueOf(maxTQ1));
		this.minTempQ1 = new Text(String.valueOf(minTQ1));
	
		this.maxTempGQ = new Text(String.valueOf(maxTGQ));
		this.minTempGQ = new Text(String.valueOf(minTGQ));
	
		this.maxTempNQ = new Text(String.valueOf(maxTNQ));
		this.minTempNQ = new Text(String.valueOf(minTNQ));
		
		this.airTemperature = new Text();
		this.quality = new Text();
		
	}
	
	public Text getAirTemperature(){
		return this.airTemperature;
	}
	
	public Text getQuality(){
		return this.quality;
	}
	
	public Text getMaxTempQ1(){
		return maxTempQ1;
	}
	
	public Text getMinTempQ1(){
		return minTempQ1;
	}

	public Text getMaxTempGQ(){
		return maxTempGQ;
	}
	
	public Text getMinTempGQ(){
		return minTempGQ;
	}
	
	public Text getMaxTempNQ(){
		return maxTempNQ;
	}
	
	public Text getMinTempNQ(){
		return minTempNQ;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
	    
		airTemperature.readFields(in);
		quality.readFields(in);
		
		maxTempQ1.readFields(in);
		minTempQ1.readFields(in);
	
		maxTempGQ.readFields(in);
		minTempGQ.readFields(in);
	
		maxTempNQ.readFields(in);
		minTempNQ.readFields(in);
	
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
	   
	    airTemperature.write(out);
		quality.write(out);
		
		maxTempQ1.write(out);
		minTempQ1.write(out);
	
		maxTempGQ.write(out);
		minTempGQ.write(out);
	
		maxTempNQ.write(out);
		minTempNQ.write(out);

	}
	
	public int compareTo(TempAndQuality tp) {
	    
	    int cmp=airTemperature.compareTo(tp.getAirTemperature());
	    if(cmp!=0)
	        return cmp;
	    return quality.compareTo(tp.getQuality());
	    
	}
	
	public double getMean(){

		
		return ((getDoubleOf(maxTempQ1)/10) + (getDoubleOf(minTempQ1)/10) + (getDoubleOf(maxTempGQ)/10) + (getDoubleOf(minTempGQ)/10) + (getDoubleOf(maxTempNQ)/10) + (getDoubleOf(minTempNQ)/10) )/6;
	
	}
	
	public double getDoubleOf(Text d){
	
		return Double.valueOf(d.toString());
	
	}
	
	@Override
	public String toString(){
	
	String MtQ1 = "\nMax Temperature with quality 1: "+(getDoubleOf(maxTempQ1)/10)+"ºC";
	String mtQ1 = "\nMin Temperature with quality 1: "+(getDoubleOf(minTempQ1)/10)+"ºC";
	String MtGQ = "\nMax Temperature with quality in [01459]: "+(getDoubleOf(maxTempGQ)/10)+"ºC";
	String mtGQ = "\nMin Temperature with quality in [01459]: "+(getDoubleOf(minTempGQ)/10)+"ºC";
	String MtNQ = "\nMax Temperature without having quality into account: "+(getDoubleOf(maxTempNQ)/10)+"ºC";
	String mtNQ = "\nMin Temperature without having quality into account: "+(getDoubleOf(minTempNQ)/10)+"ºC";
	String mean = "\nMEAN TEMPERATURE IS: "+String.format("%.2f", getMean())+"ºC\n";
	
	return MtQ1+mtQ1+MtGQ+mtGQ+MtNQ+mtNQ+mean;
	
	
	}	


}
