package busyTime;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PeakPriceMapper extends Mapper<LongWritable, Text, NullWritable, Text>{

//	Input to this mapper :
//	
//	1	63181,182.79967
//	2	56810,180.96103
//	3	55181,181.81874
//	4	51382,197.25288
//	5	53600,193.7123
//	6	52731,196.53531
//	7	54963,202.48631
//	8	55561,203.33014
//	9	33398,237.04773
//	10	46345,233.41624
//	11	58888,202.92442
//	12	60997,192.60191
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		Text outValue = new Text();
		try
		{
			String in[] = value.toString().split("\\t");
			String input[] = in[1].split(",");
			
			String val = ""+in[0]+"::"+input[0]+"::"+input[1];
			outValue.set(val);
			context.write(NullWritable.get(), outValue);
		}
		catch(Exception e)
		{
			System.out.println("Exception in 2nd mapper " +e.getMessage());
		}
	}

}
