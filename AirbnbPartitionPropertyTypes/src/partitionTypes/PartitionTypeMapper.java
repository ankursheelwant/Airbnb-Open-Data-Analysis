package partitionTypes;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.opencsv.CSVParser;

public class PartitionTypeMapper extends Mapper<Object,Text, Text, Text>{

	private CSVParser csvReader = new CSVParser(',', '"');

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String[] record = this.csvReader.parseLine(value.toString());
		Text propertyType = new Text();
		String type;
		long listingId = 0;
		try{
			
			listingId = Long.parseLong(record[1]);
			type = record[13];
			if(!type.isEmpty())
			{
				propertyType.set(type);
				context.write(propertyType, value);
			}
			
		}
		catch(Exception e)
		{
			System.out.println("Error in mapper: "+e.getMessage());
		}
		
		
	}

}
