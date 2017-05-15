package busyTime;

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class BusiestMonthReducer extends Reducer<NullWritable,Text,NullWritable,Text> {

	SortedMap<Long,Integer> map = new TreeMap<>();
	int n = 3;
	@Override
	protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		for(Text value : values)
		{
			String val[] = value.toString().split("::");
			
			map.put(Long.parseLong(val[1]), Integer.parseInt(val[0]));
			if(map.size()>3)
			{
				map.remove(map.firstKey());
			}
		}
		
		
		for(Entry eachEntry : map.entrySet())
		{
			String outVal = ""+eachEntry.getValue()+","+eachEntry.getKey()+",visitors.";
			context.write(NullWritable.get(), new Text(outVal));
		}
		
	}


}
