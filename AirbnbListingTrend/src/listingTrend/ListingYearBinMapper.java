package listingTrend;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.opencsv.CSVParser;



public class ListingYearBinMapper extends Mapper<Object,Text, Text, NullWritable>{

	private CSVParser csvReader = new CSVParser(',', '"');
	
	private MultipleOutputs<Text, NullWritable> multops;
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		multops = new MultipleOutputs(context);
	}

	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String[] record = this.csvReader.parseLine(value.toString());
		
		String year = record[6].substring(0, 4);
		String val = year + "::" + record[1] + "::" + record[2];
		
        multops.write("YearBin", new Text(val), NullWritable.get(), year);
            
	}

	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		multops.close();
	}
}
