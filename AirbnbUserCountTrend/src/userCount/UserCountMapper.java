package userCount;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.opencsv.CSVParser;

public class UserCountMapper extends Mapper<Object,Text, IntWritable, IntWritable>{

	private CSVParser csvReader = new CSVParser(',', '"');
	private Set<Long> set = Collections.synchronizedSet(new HashSet<>());

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String[] record = this.csvReader.parseLine(value.toString());
		int year = 0;
		long userId = 0;
		try{
			year = Integer.parseInt(record[3].substring(0, 4));
			userId = Long.parseLong(record[4]);
			if(set.contains(userId))
			{
				return;
			}
			
			set.add(userId);
			
			IntWritable yyyy = new IntWritable(year);
			
			context.write(yyyy, new IntWritable(1));
		}
		catch(Exception e)
		{
			System.out.println(""+e.getMessage());
		}
		
	}
	

}
