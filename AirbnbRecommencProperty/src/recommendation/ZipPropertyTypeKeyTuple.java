package recommendation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class ZipPropertyTypeKeyTuple implements Writable, WritableComparable<ZipPropertyTypeKeyTuple>{

	String zip;
	String type;
	
	public ZipPropertyTypeKeyTuple()
	{
		
	}
	public ZipPropertyTypeKeyTuple(String zip, String type) {
		super();
		this.zip = zip;
		this.type = type;
	}
	
	public String getZip() {
		return zip;
	}

	public void setZip(String zip) {
		this.zip = zip;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	@Override
	public void readFields(DataInput di) throws IOException {
		// TODO Auto-generated method stub
		zip = WritableUtils.readString(di);
		type = WritableUtils.readString(di);
	}

	@Override
	public void write(DataOutput d) throws IOException {
		// TODO Auto-generated method stub
		WritableUtils.writeString(d, zip);
		WritableUtils.writeString(d, type);
	}

	

	@Override
	public int compareTo(ZipPropertyTypeKeyTuple o) {
		int result = zip.compareTo(o.zip);
        if(result ==0)
        {
            result = type.compareTo(o.type);
        }
        return result;
	}
	@Override
	public String toString() {
		return ""+zip + "," + type;
	}

	
}
