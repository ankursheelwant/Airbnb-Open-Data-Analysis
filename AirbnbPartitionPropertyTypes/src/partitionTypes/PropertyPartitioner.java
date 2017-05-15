package partitionTypes;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PropertyPartitioner extends Partitioner<Text, Text> implements Configurable{

	private Configuration conf;
	@Override
	public Configuration getConf() {
		// TODO Auto-generated method stub
		return conf;
	}

	@Override
	public void setConf(Configuration arg0) {
		// TODO Auto-generated method stub
		conf = arg0;
	}

	@Override
	public int getPartition(Text type, Text val, int arg2) {
		// TODO Auto-generated method stub
		if(type.toString().equals("apartment"))
			return 0;
		else if(type.toString().equals("bed  breakfast"))
			return 1;
		else if(type.toString().equals("boat"))
			return 2;
		else if(type.toString().equals("condominium"))
			return 3;
		else if(type.toString().equals("house"))
			return 4;
		else if(type.toString().equals("loft"))
			return 5;
		else if(type.toString().equals("villa"))
			return 6;
		else if(type.toString().equals("townhouse"))
			return 7;
		else
			return 8;

	}

}
