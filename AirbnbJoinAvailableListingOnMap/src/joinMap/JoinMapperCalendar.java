package joinMap;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.opencsv.CSVParser;

public class JoinMapperCalendar extends Mapper<Object, Text, Text, Text> {

    private Text outkey = new Text();
    private Text outvalue = new Text();

    private Set<Long> set = new HashSet<>();
    private CSVParser csvReader = new CSVParser(',', '"');
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Parse the input string into a nice map
    	String[] record = this.csvReader.parseLine(value.toString());
    	try
    	{
	        String listingId = record[0];
	        if (listingId == null) {
	            return;
	        }
	        
	        if(!set.contains(Long.parseLong(listingId)) && record[2].equalsIgnoreCase("t"))
	        {
	        	set.add(Long.parseLong(listingId));
	        	String val = record[2];
		        outkey.set(listingId);
		        // Flag this record for the reducer and then output
		        outvalue.set("B" + val);
		        context.write(outkey, outvalue);
	        }
	        
    	}
    	catch(Exception e)
    	{
    		System.out.println("exception in JoinMapperListing: "+e.getLocalizedMessage());
    	}
    }
}