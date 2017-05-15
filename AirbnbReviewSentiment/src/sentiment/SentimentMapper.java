package sentiment;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.filecache.DistributedCache;

import com.opencsv.CSVParser;

public class SentimentMapper extends Mapper<Object,Text, LongWritable, FloatWritable>{

	private CSVParser csvReader = new CSVParser(',', '"');

	private Map<String, Integer> dictionary;
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		dictionary = new HashMap<String, Integer>();
		
		int score;
        String word;
        
        
        try {
            Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            if (files != null && files.length > 0) {

                for (Path file : files) {

                    try {
                        //File myFile = new File(file.toUri());
                    	//if(file.equals("words.txt"))
                    	//{
	                        BufferedReader bufferedReader = new BufferedReader(new FileReader("words.txt"));
	                        String line = null;
	                        while ((line = bufferedReader.readLine()) != null) {
	                            String[] split = line.split("\\t");
	
	                            word = split[0];
	                            score = Integer.parseInt(split[1]);
	                            dictionary.put(word, score);
	                        }
	                        System.out.println("Dictionary size is: "+dictionary.size());
                        //}
                    } catch (IOException ex) {
                        System.err.println("Exception while reading  file: " + ex.getMessage());
                    }
                }
            }
        } catch (IOException ex) {
            System.err.println("Exception in mapper setup: " + ex.getMessage());
        }
	}


	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String[] record = this.csvReader.parseLine(value.toString());
		
		long listingId = 0;
		String comment;
		int score = 0;
		float avgScore = 0;
		LongWritable listing = new LongWritable();
		
		try{
			listingId = Long.parseLong(record[1]);
			
			comment = record[6];
			String[] commentWords = comment.split(" ");
			int count = 0;
			String temp;
			
			for(String s: commentWords)
			{
				
				if(dictionary.containsKey(s))
				{
					score += dictionary.get(s);
					count++;
				}
			}
			
			avgScore = score/count;
			listing.set(listingId);
			
			context.write(listing, new FloatWritable(avgScore));
		}
		catch(Exception e)
		{
			System.out.println("Error in mapper"+e.getMessage());
		}
		
		
	}

}
