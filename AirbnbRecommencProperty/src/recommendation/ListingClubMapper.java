package recommendation;

import java.io.IOException;
import java.text.NumberFormat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.opencsv.CSVParser;



public class ListingClubMapper extends Mapper<Object,Text, ZipPropertyTypeKeyTuple, ListingReviewScoreValueTuple>{

	private CSVParser csvReader = new CSVParser(',', '"');
	
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String[] record = this.csvReader.parseLine(value.toString());
		
		String zip;
		String propertyType;
		
		String reviewCount;
		String score;
		String listingId;
		
		try{
			ZipPropertyTypeKeyTuple outKey = null;
			ListingReviewScoreValueTuple outVal = null;
			
			if(!record[9].isEmpty() && !record[13].isEmpty())
			{
				zip = record[9];
				propertyType = record[13];
				outKey = new ZipPropertyTypeKeyTuple(zip, propertyType);
			}
			if(!record[1].isEmpty() && (Integer.parseInt(record[15])!=0) && !record[16].equalsIgnoreCase("na"))
			{
				listingId = record[1];
				reviewCount = record[15];
				score = record[16];
				int reviewScore = Integer.parseInt(reviewCount) * Integer.parseInt(score);
				outVal = new ListingReviewScoreValueTuple(String.valueOf(reviewScore), listingId);
			}
			if(outKey!=null && outVal!=null)
				context.write(outKey, outVal);
			
		}
		catch(Exception e)
		{
			System.out.println("Exception in ClubMapper: "+e.getMessage());
		}
				
	}	
}
