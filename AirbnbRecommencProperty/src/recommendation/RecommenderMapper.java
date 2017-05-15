package recommendation;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.opencsv.CSVParser;

public class RecommenderMapper extends Mapper<Object,Text, Text, Text>{

	
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
	
		try{
			String[] listingScorePairs = value.toString().split("::");
			
			for(String pairs: listingScorePairs)
			{
				String[] pairValues = pairs.split(",");
				String listingId = pairValues[1];
				
				for(String pairs2: listingScorePairs)
				{
					if(!pairs.equals(pairs2))
						context.write(new Text(listingId), new Text(pairs2));
				}
			}
		}
		catch(Exception e)
		{
			System.out.println("Exception in RecommenderMapper: "+e.getMessage());
		}
	}
}
