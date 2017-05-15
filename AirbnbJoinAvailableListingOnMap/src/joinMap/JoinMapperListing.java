package joinMap;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.opencsv.CSVParser;

public class JoinMapperListing extends Mapper<Object, Text, Text, Text> {

    private Text outkey = new Text();
    private Text outvalue = new Text();

    private CSVParser csvReader = new CSVParser(',', '"');
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Parse the input string into a nice map
    	String[] record = this.csvReader.parseLine(value.toString());
    	try
    	{
	        String listingId = record[1];
	        if (listingId == null) {
	            return;
	        }
	        
	        String val = record[13]+","+record[11]+","+record[12];
	        outkey.set(listingId);
	        // Flag this record for the reducer and then output
	        outvalue.set("A" + val);
	        context.write(outkey, outvalue);
    	}
    	catch(Exception e)
    	{
    		System.out.println("exception in JoinMapperListing: "+e.getMessage());
    	}
    }
}