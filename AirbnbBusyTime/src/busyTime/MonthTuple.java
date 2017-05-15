package busyTime;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class MonthTuple implements Writable{

	String listingId;
	String price;
	
	public MonthTuple()
	{
		
	}
	public MonthTuple(String listingId, String price) {
		this.listingId = listingId;
		this.price = price;
	}
	public String getListingId() {
		return listingId;
	}
	public void setListingId(String listingId) {
		this.listingId = listingId;
	}
	public String getPrice() {
		return price;
	}
	public void setPrice(String price) {
		this.price = price;
	}
	@Override
	public String toString() {
		return "" + listingId + "," + price;
	}
	@Override
	public void readFields(DataInput di) throws IOException {
		// TODO Auto-generated method stub
		listingId = WritableUtils.readString(di);
		price = WritableUtils.readString(di);
		
	}
	@Override
	public void write(DataOutput d) throws IOException {
		// TODO Auto-generated method stub
		WritableUtils.writeString(d, listingId);
		WritableUtils.writeString(d, price);
		
	}
	
	
}
