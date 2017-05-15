package joinMap;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;



public class JoinReducer extends Reducer<Text,Text,Text,NullWritable> {


    private Text tmp = new Text();
    private ArrayList<Text> listA = new ArrayList<Text>();
    private ArrayList<Text> listB = new ArrayList<Text>();


    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Clear our lists
        listA.clear();
        listB.clear();
        
        
        while (values.iterator().hasNext()) {
            tmp = values.iterator().next();
            System.out.println(Character.toString((char) tmp.charAt(0)));
            if (Character.toString((char) tmp.charAt(0)).equals("A")) {
                listA.add(new Text(tmp.toString().substring(1)));
            }
            if (Character.toString((char) tmp.charAt(0)).equals("B")) {
                 listB.add(new Text(tmp.toString().substring(1)));
            }
           
        }
        
        if (!listA.isEmpty() && !listB.isEmpty()) {
            
            for (Text A : listA) {
                //System.out.println("here1");
                for (Text B : listB) {
                    //System.out.println("here2");
                    context.write(A, NullWritable.get());
                }
            }
        }
    }
}