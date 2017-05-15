package recommendation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.w3c.dom.stylesheets.LinkStyle;

public class ListingReviewScoreValueTuple implements Writable{

	String reviewScore;
	String listingId;
	
	public ListingReviewScoreValueTuple()
	{
		
	}
	public ListingReviewScoreValueTuple(String reviewScore, String listingId) {
		this.reviewScore = reviewScore;
		this.listingId = listingId;
	}

	public String getReviewScore() {
		return reviewScore;
	}

	public void setReviewScore(String reviewScore) {
		this.reviewScore = reviewScore;
	}

	public String getListingId() {
		return listingId;
	}

	public void setListingId(String listingId) {
		this.listingId = listingId;
	}

	@Override
	public void readFields(DataInput di) throws IOException {
		// TODO Auto-generated method stub
		reviewScore = WritableUtils.readString(di);
		listingId = WritableUtils.readString(di);
	}

	@Override
	public void write(DataOutput d) throws IOException {
		// TODO Auto-generated method stub
		WritableUtils.writeString(d, reviewScore);
		WritableUtils.writeString(d, listingId);
	}
	@Override
	public String toString() {
		return "" + reviewScore + "," + listingId;
	}

}
