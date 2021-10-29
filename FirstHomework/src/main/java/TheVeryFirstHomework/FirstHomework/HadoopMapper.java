package TheVeryFirstHomework.FirstHomework;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class HadoopMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>{

	DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd");
	
	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
			
			if(key.get() == 0) 
				return;
			
			Date date = new Date();
			dateFormat.format(date);
			
			String line = value.toString();
			
			String[] tokens = line.split(",");
			String[] dateTokens = line.split(date.toString());
			
			String paymentTypeCol = tokens[3].trim();
			String productCol = tokens[1].trim();
			String saleDateCol = dateTokens[0].split(" ")[0].trim();
			
			output.collect(new Text(paymentTypeCol), new IntWritable(1));
			output.collect(new Text(productCol), new IntWritable(1));
			output.collect(new Text(saleDateCol), new IntWritable(1));
		}
	}

