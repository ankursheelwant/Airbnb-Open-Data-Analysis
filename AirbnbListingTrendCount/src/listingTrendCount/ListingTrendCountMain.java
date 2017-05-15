package listingTrendCount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import listingTrendCount.ListingTrendCountMapper;
import listingTrendCount.ListingTrendCountReducer;
import listingTrendCount.ListingTrendCountMain;

public class ListingTrendCountMain {

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
        Job job=Job.getInstance(conf, "Calculate count for each year");
        job.setJarByClass(ListingTrendCountMain.class);
        job.setMapperClass(ListingTrendCountMapper.class);
        job.setCombinerClass(ListingTrendCountReducer.class);
        job.setReducerClass(ListingTrendCountReducer.class);
        
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new  Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
 
}
