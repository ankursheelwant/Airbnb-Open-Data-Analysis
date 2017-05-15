package listingTrendCount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.opencsv.CSVParser;



public class ListingTrendCountMapper extends Mapper<Object,Text, IntWritable, IntWritable>{

	private CSVParser csvReader = new CSVParser(',', '"');

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String[] record = this.csvReader.parseLine(value.toString());
		int year = 0;
		try{
			year = Integer.parseInt(record[6].substring(0, 4));
			IntWritable yyyy = new IntWritable(year);
			
			context.write(yyyy, new IntWritable(1));
		}
		catch(Exception e)
		{
			System.out.println(""+e.getMessage());
		}
		
		
	}

}
