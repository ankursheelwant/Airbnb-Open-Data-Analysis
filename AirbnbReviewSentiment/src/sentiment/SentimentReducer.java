package sentiment;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class SentimentReducer extends Reducer<LongWritable,FloatWritable,LongWritable,Text> {

	@Override
	protected void reduce(LongWritable key, Iterable<FloatWritable> values,Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		Text sentiResult = new Text();
		
		int count = 0;
		float score = 0;
		float finalScore = 0;
		
		for(FloatWritable val : values)
		{
			score += val.get();
			count++;
		}
		
		finalScore = score/count;
		
		if(finalScore>0)
			sentiResult.set("Positive");
		else if(finalScore==0)
			sentiResult.set("Neutral");
		else
			sentiResult.set("Negative");
		
		context.write(key,sentiResult);
	}

	
}
