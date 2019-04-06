import java.util.*; 
import java.io.IOException;
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.conf.*; 
import org.apache.hadoop.io.*; 
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
public class MapSplit
{
   public static class Map extends Mapper <LongWritable, Text, Text, Text>
   {
    	  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
    	  {
    	  	  String line[]=value.toString().split(","); 		   
    		  context.write(new Text(line[1]),new Text(line[3]));	
    	  } 
   }
    public static void main(String args[])throws Exception 
    {
       Configuration conf=new Configuration();
       conf.set("mapreduce.max.split.size","5000");
       Job job = new Job(conf,"Two");
       job.setJarByClass(MapSplit.class);
       job.setOutputKeyClass(Text.class);
       job.setOutputValueClass(Text.class);
       job.setMapperClass(Map.class);
       job.setInputFormatClass(TextInputFormat.class);
       job.setOutputFormatClass(TextOutputFormat.class);
       FileInputFormat.addInputPath(job,new Path(args[0]));
       FileOutputFormat.setOutputPath(job,new Path(args[1]));
       job.waitForCompletion(true); 
    } 
 } 
