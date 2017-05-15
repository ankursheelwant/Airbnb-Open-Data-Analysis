package partitionTypes;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import partitionTypes.*;

public class PartitionMain {

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
        Job job=Job.getInstance(conf, "Partitioning of proprties types");
        job.setJarByClass(PartitionMain.class);
        job.setMapperClass(PartitionTypeMapper.class);
        job.setReducerClass(PartitionTypeReducer.class);
        job.setPartitionerClass(PropertyPartitioner.class);
        
        job.setNumReduceTasks(9);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new  Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
