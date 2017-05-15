package recommendation;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;



public class ListingClubReducer extends Reducer<ZipPropertyTypeKeyTuple,ListingReviewScoreValueTuple,ZipPropertyTypeKeyTuple,Text> {

	
	@Override
	protected void reduce(ZipPropertyTypeKeyTuple key, Iterable<ListingReviewScoreValueTuple> values, Context context) 
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		try
		{
			//ArrayListOfListingsScoreValueTuple listOfValue = new ArrayListOfListingsScoreValueTuple();
			ArrayList<String> list = new ArrayList<>();
			//System.out.println("PRINTING LIST######");
			for (ListingReviewScoreValueTuple val: values) {
				//System.out.println(""+val);
				list.add(val.toString());
			}
			
			StringBuffer sb = new StringBuffer();
			for(String l : list){
				sb.append(l + "::");
			}
			
			context.write(key, new Text(sb.toString()));
		}
		catch(Exception e)
		{
			System.out.println("Exception in LIstingClubReducer: "+e.getMessage());
		}
	}	
}