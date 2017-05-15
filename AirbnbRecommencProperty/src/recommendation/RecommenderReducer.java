package recommendation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class RecommenderReducer extends Reducer<Text,Text,Text,Text> {

	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		try
		{
			SortedMap<Integer,String> map = new TreeMap<>();
			for(Text val: values)
			{
				String[] pair = val.toString().split(",");
				String listingId = pair[1];
				int score = Integer.parseInt(pair[0]);
				map.put(score, listingId);
				if(map.size()>5)
				{
					map.remove(map.firstKey());
				}
			}
			StringBuffer outBuffer = new StringBuffer("");//Recommended Listing IDs are: 
			for(Entry<Integer, String> ent: map.entrySet())
			{
				outBuffer.append(ent.getValue()+", ");
			}
			
			String recommendation = outBuffer.toString();
			recommendation = recommendation.substring(0,recommendation.length()-2);
			
			context.write(key, new Text(recommendation));
		}
		catch(Exception e)
		{
			System.out.println("Exception in RecommenderReducer: "+e.getMessage());
		}
	}	
}