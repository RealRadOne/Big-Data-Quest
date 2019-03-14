import java.util.*;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Tent
{
	public static class Map extends Mapper<LongWritable, Text,NullWritable,Text>
	{
       	private TreeMap<Integer, Text> grade = new TreeMap<Integer, Text>();

   public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
    	String[] line = value.toString().trim().split(",");
    	int i=Integer.parseInt(line[3]);
    	grade.put(i,new Text(value));
    	if (grade.size() > 10)
    	{
    	grade.remove(grade.firstKey());
    	} 	 
	}
   protected void cleanup(Context context) throws IOException, InterruptedException
	{          	 
    	for (Text name : grade.values() )
    	{
               	context.write(NullWritable.get(), name);
    	}
	}
   }

   public static class Reduce extends Reducer<NullWritable, Text, NullWritable, Text>
  {    	 
  public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
  {
    	TreeMap<Integer, Text> grade = new TreeMap< Integer, Text>();
    	for(Text value : values)
    	{
   			String[] line = value.toString().split(",");
           	int i=Integer.parseInt(line[3]);
           	grade.put(i, new Text(value));
           	if (grade.size() > 10)
           	{
            	grade.remove(grade.firstKey());
           	}
     	}
     	for(Text t : grade.values())
     	{
        	context.write(NullWritable.get(), t);
     	}
   }
   }
   public static void main(String args[])throws Exception
   {
	Configuration conf=new Configuration();
	Job job = new Job(conf,"Two");
	job.setJarByClass(Tent.class);
	job.setOutputKeyClass(NullWritable.class);
	job.setOutputValueClass(Text.class);
	job.setMapperClass(Map.class);
	job.setReducerClass(Reduce.class);
	job.setNumReduceTasks(1);
	job.setInputFormatClass(TextInputFormat.class);
	job.setOutputFormatClass(TextOutputFormat.class);
	FileInputFormat.addInputPath(job,new Path(args[0]));
	FileOutputFormat.setOutputPath(job,new Path(args[1]));
	job.waitForCompletion(true);
 }

}

