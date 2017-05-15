package userCount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class UserCountReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {

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
