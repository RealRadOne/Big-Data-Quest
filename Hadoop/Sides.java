import java.util.*; 
import java.io.IOException;
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.conf.*; 
import org.apache.hadoop.io.*; 
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
public class Sides 
{
	public static class Map extends Mapper<LongWritable,Text,Text,Text>
	{
	    public void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException	
	    {
	    String name1=context.getConfiguration().get("name");
		String[] line=value.toString().split(",");
		if(name1.equals(line[1])==true)
		{
		context.write(new Text(line[0]+" "+line[1]),new Text(line[2]+","+line[3]+","+line[4]+","+line[5]+","+line[6]));	
	    }	
	    }
	}
	public static void main(String args[])throws Exception
	{
		Configuration conf=new Configuration();
		conf.set("name",args[2]);
		Job job=new Job(conf,"Sides");
		job.setJarByClass(Sides.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(Map.class);
		job.setNumReduceTasks(0);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		job.waitForCompletion(true);	
	}
}

