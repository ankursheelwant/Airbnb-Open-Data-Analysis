package busyTime;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PeakPriceReducer extends Reducer<NullWritable,Text,NullWritable,Text> {

	SortedMap<Float,Integer> map = new TreeMap<>();
	int n = 3;
	@Override
	protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		long count = 0;
		double runningTotal = 0;
		float totalAvg = 0;
		
		for(Text value : values)
		{
			String val[] = value.toString().split("::");
			runningTotal += Long.parseLong(val[1]) * Float.parseFloat(val[2]);
			count += Long.parseLong(val[1]);
			
			map.put(Float.parseFloat(val[2]), Integer.parseInt(val[0]));
			if(map.size()>3)
			{
				map.remove(map.firstKey());
			}
		}
		
		totalAvg = (float) (runningTotal/count);
		
		for(Entry eachEntry : map.entrySet())
		{
			float spike = (float)eachEntry.getKey() - totalAvg;
			String outVal = "Month:"+eachEntry.getValue()+" has average price: $"+eachEntry.getKey()+" means $"+ spike +" more than normal average.";
			context.write(NullWritable.get(), new Text(outVal));
		}
		
	}

}
