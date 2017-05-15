package listingTrendCount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;


public class ListingTrendCountReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {

	@Override
	protected void reduce(IntWritable key, Iterable<IntWritable> values,Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		int count = 0;
		for(IntWritable val : values)
		{
			count += val.get();
		}
		
		context.write(key,new IntWritable(count));
	}

	
}
