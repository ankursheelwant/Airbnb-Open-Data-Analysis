package busyTime;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import busyTime.BusyTimeMapper;
import busyTime.BusyTimeReducer;
import busyTime.BusyTimeMain;
import busyTime.MonthTuple;


public class BusyTimeMain {

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
        Job job=Job.getInstance(conf, "Calculate count and avg for each month");
        job.setJarByClass(BusyTimeMain.class);
        //job.setJar("wordCountLab1.jar");
        job.setMapperClass(BusyTimeMapper.class);
        //job.setCombinerClass(BusyTimeReducer.class);
        job.setReducerClass(BusyTimeReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(MonthTuple.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new  Path(args[1]));
        boolean complete = job.waitForCompletion(true);
        
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Get price spike for top 3 months");
        
        Configuration conf3 = new Configuration();
        Job job3 = Job.getInstance(conf3, "Get busiest 3 months");
        
        if(complete)
        {
        	job2.setJarByClass(BusyTimeMain.class);
        	
            job2.setMapperClass(PeakPriceMapper.class);
            job2.setReducerClass(PeakPriceReducer.class);
            
            job2.setOutputKeyClass(NullWritable.class);
            job2.setOutputValueClass(Text.class);
            
            FileInputFormat.addInputPath(job2, new Path(args[1]));
            FileOutputFormat.setOutputPath(job2,new  Path(args[2]));
            
            job2.waitForCompletion(true);
            
            
            //For job3 Busiest 3 Months
            job3.setJarByClass(BusyTimeMain.class);
        	
            job3.setMapperClass(PeakPriceMapper.class);
            job3.setReducerClass(BusiestMonthReducer.class);
            
            job3.setOutputKeyClass(NullWritable.class);
            job3.setOutputValueClass(Text.class);
            
            FileInputFormat.addInputPath(job3, new Path(args[1]));
            FileOutputFormat.setOutputPath(job3,new  Path(args[3]));
            
            System.exit(job3.waitForCompletion(true) ? 0 : 1);
        }
	}

}
