package sentiment;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import sentiment.SentimentMapper;
import sentiment.SentimentReducer;

public class SentimentMain {


	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
        Job job=Job.getInstance(conf, "Calculate count for each year");
        job.setJarByClass(SentimentMain.class);
        job.setMapperClass(SentimentMapper.class);
        //job.setCombinerClass(ListingTrendCountReducer.class);
        job.setReducerClass(SentimentReducer.class);
        
        DistributedCache.addCacheFile(new URI(args[2]), job.getConfiguration());
        
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(FloatWritable.class);
        
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new  Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	 
}
