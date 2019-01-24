import java.util.*; 
import java.io.IOException;
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.conf.*; 
import org.apache.hadoop.io.*; 
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
public class Two
{
   public static class Map extends Mapper <LongWritable, Text, Text, IntWritable>
   {
	      public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException 
	      { 
	         String[] line = value.toString().split(",");
	         int G1=Integer.parseInt(line[8]);
	         context.write(new Text(line[0]),new IntWritable(G1));
	       } 
   }
   public static class dpart extends Partitioner<Text,IntWritable>
   {
 	  public int getPartition(Text key,IntWritable value,int nr)
 	  {
 		 if(value.get()>8 && value.get()<10)
 			  return 0;
 		 if(value.get()>10 && value.get()<15)
 		 	  return 1;
 		  else
 			  return 2;
 	  }
    }
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> 
    {
    	public void reduce(Text key, IntWritable value, Context context) throws IOException, InterruptedException
		{	
    	context.write(key, value);
		}
    }
    public static void main(String args[])throws Exception 
    {
       Configuration conf=new Configuration();
       Job job = new Job(conf,"Two");
       job.setJarByClass(Two.class); 
       job.setJarByClass(Two.class); 
       job.setOutputKeyClass(Text.class);
       job.setOutputValueClass(IntWritable.class);
       job.setMapperClass(Map.class);
       job.setPartitionerClass(dpart.class);
       job.setReducerClass(Reduce.class);
       job.setNumReduceTasks(3);
       job.setInputFormatClass(TextInputFormat.class);
       job.setOutputFormatClass(TextOutputFormat.class);
       FileInputFormat.addInputPath(job,new Path(args[0]));
       FileOutputFormat.setOutputPath(job,new Path(args[1]));
       job.waitForCompletion(true); 
    } 
 } 