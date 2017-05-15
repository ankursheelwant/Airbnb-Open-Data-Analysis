package recommendation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Writable;

public class ArrayListOfListingsScoreValueTuple extends ArrayList<ListingReviewScoreValueTuple> implements Writable {

	public ArrayListOfListingsScoreValueTuple()
	{
		
	}
	
	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public String toString() {
		Iterator<ListingReviewScoreValueTuple> itr = this.iterator();
		StringBuffer buff = new StringBuffer("");
		while(itr.hasNext())
		{
			ListingReviewScoreValueTuple tup = itr.next();
			buff = buff.append(tup+"::");
		}
		return buff.toString();
	}

//	public static void main(String[] args)
//	{
//		ArrayListOfListingsScoreValueTuple list = new ArrayListOfListingsScoreValueTuple();
//		list.add(new ListingReviewScoreValueTuple("1", "1"));
//		list.add(new ListingReviewScoreValueTuple("2", "2"));
//		list.add(new ListingReviewScoreValueTuple("3", "3"));
//		list.add(new ListingReviewScoreValueTuple("4", "4"));
//		
//		System.out.println(list);
//	}
	
}
