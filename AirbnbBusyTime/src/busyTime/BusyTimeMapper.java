package busyTime;

import java.io.IOException;
import java.text.NumberFormat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.opencsv.CSVParser;

public class BusyTimeMapper extends Mapper<Object,Text, IntWritable, MonthTuple>{

	private CSVParser csvReader = new CSVParser(',', '"');
	IntWritable month = new IntWritable();	
	MonthTuple monthTuple;
	
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String[] record = this.csvReader.parseLine(value.toString());
		String date = "", listingIdString = "", priceString = "";
		long listingId = 0;
		float price = 0;
		
		if(record.length>3)
		{
			date = record[1];			
			listingIdString = record[0];
			priceString = record[3];

			try{
				month.set(Integer.parseInt(date.substring(5, 7)));
				listingId = Long.parseLong(listingIdString);
				if(!priceString.isEmpty())
				{
					NumberFormat format = NumberFormat.getCurrencyInstance();
					Number cost = format.parse(priceString);
					price = cost.floatValue();
				}
				else
					return;
			}
			catch(Exception e)
			{
				System.out.println(""+e.getMessage());
			}
			
			monthTuple = new MonthTuple(String.valueOf(listingId), String.valueOf(price));
			
			context.write(month, monthTuple);
		}
				
	}	
}
