package busyTime;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BusyTimeReducer extends Reducer<IntWritable,MonthTuple,IntWritable,MonthTuple> {

	MonthTuple monthTuple;
	
	@Override
	protected void reduce(IntWritable key, Iterable<MonthTuple> values, Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		long count = 0;
		float avg = 0;
		float total = 0;
		
		for(MonthTuple tuple : values)
		{
			total += Float.parseFloat(tuple.getPrice());
			count++;
		}
		try{
			avg = total/count;
		}
		catch(ArithmeticException e)
		{
			System.out.println("Arithmetic Error: "+e.getMessage());
		}
		catch(Exception e)
		{
			System.out.println(""+e.getMessage());
		}
	
		monthTuple = new MonthTuple(String.valueOf(count), String.valueOf(avg));
		
		context.write(key, monthTuple);
	}	
}
