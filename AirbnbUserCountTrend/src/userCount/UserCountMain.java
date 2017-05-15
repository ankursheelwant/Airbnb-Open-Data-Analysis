package userCount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import userCount.UserCountMapper;
import userCount.UserCountReducer;

public class UserCountMain {

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
        Job job=Job.getInstance(conf, "Count of new users per year");
        job.setJarByClass(UserCountMain.class);
        job.setMapperClass(UserCountMapper.class);
        job.setCombinerClass(UserCountReducer.class);
        job.setReducerClass(UserCountReducer.class);
        
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new  Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
