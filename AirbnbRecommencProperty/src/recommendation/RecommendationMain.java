package recommendation;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import recommendation.*;

public class RecommendationMain {

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
        Job job=Job.getInstance(conf, "Recommendation System");
        job.setJarByClass(RecommendationMain.class);
        
        job.setMapperClass(ListingClubMapper.class);
        //job.setCombinerClass(BusyTimeReducer.class);
        job.setReducerClass(ListingClubReducer.class);
        
        job.setMapOutputKeyClass(ZipPropertyTypeKeyTuple.class);
        job.setMapOutputValueClass(ListingReviewScoreValueTuple.class);
        job.setOutputKeyClass(ZipPropertyTypeKeyTuple.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new  Path(args[1]));
        boolean complete = job.waitForCompletion(true);
        
        
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Delivering Recommendations");
        
        if(complete)
        {
        	job2.setJarByClass(RecommendationMain.class);
        	
            job2.setMapperClass(RecommenderMapper.class);
            job2.setReducerClass(RecommenderReducer.class);
            
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            
            FileInputFormat.addInputPath(job2, new Path(args[1]));
            FileOutputFormat.setOutputPath(job2,new  Path(args[2]));
            
            job2.waitForCompletion(true);
            
            
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
	}

}
