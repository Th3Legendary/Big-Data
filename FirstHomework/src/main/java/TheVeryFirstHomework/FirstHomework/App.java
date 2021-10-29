package TheVeryFirstHomework.FirstHomework;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextOutputFormat;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws IOException
    {
    	Configuration config = new Configuration();
    	
    	Path inputFile = new Path("hdfs://127.0.0.1:9000/input/SalesJan2009.csv");
    	Path outputFile = new Path("hdfs://127.0.0.1:9000/output/result");
    	
    	JobConf jobConf = new JobConf(config, App.class);
    	
    	jobConf.setJarByClass(App.class);
    	
    	jobConf.setMapOutputKeyClass(Text.class);
    	jobConf.setMapOutputValueClass(IntWritable.class);
    	jobConf.setOutputFormat(TextOutputFormat.class);
    	jobConf.setMapperClass(HadoopMapper.class);
    	jobConf.setReducerClass(HadoopReducer.class);
    	
    	FileInputFormat.setInputPaths(jobConf, inputFile);
    	FileOutputFormat.setOutputPath(jobConf, outputFile);
    	
    	FileSystem hdfs = FileSystem.get(URI.create("hdfs://127.0.0.1:9000"), config);
    	if(hdfs.exists(outputFile)) {
    		hdfs.delete(outputFile, true);
    	}
    	
    	RunningJob job = JobClient.runJob(jobConf);
    }
}
