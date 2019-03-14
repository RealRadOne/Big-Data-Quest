import java.util.*; 
import java.io.IOException;
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.conf.*; 
import org.apache.hadoop.io.*; 
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
public class Multiples 
{
public static class Map1 extends Mapper<LongWritable,Text,Text,Text>
{
    public void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException	
    {
	String[] line=value.toString().split(",");
	context.write(new Text(line[0]),new Text(line[1]+","+line[2]));	
    }
}
public static class Map2 extends Mapper<LongWritable,Text,Text,Text>
{
    public void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException	
    {
	String[] line=value.toString().split(",");
	context.write(new Text(line[0]),new Text(line[1]));	
    }
}
public static class Reduce extends Reducer<Text,Text,Text,Text>
{
	public void reduce(Text key,Iterable<Text> values,Context context)throws IOException,InterruptedException
	{
		String line="";
		for(Text val:values)
		{
			line+=val.toString();
		}
		context.write(new Text(key),new Text(line));
	}
	
}
public static void main(String args[])throws Exception
{
	Configuration conf=new Configuration();
	Job job=new Job(conf,"Multiples");
	job.setJarByClass(Multiples.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	job.setReducerClass(Reduce.class);
	job.setInputFormatClass(TextInputFormat.class);
	job.setOutputFormatClass(TextOutputFormat.class);
	MultipleInputs.addInputPath(job,new Path(args[0]), TextInputFormat.class, Map1.class);
	MultipleInputs.addInputPath(job,new Path(args[1]), TextInputFormat.class, Map2.class);
    FileOutputFormat.setOutputPath(job,new Path(args[2]));
	job.waitForCompletion(true);	
}
}

