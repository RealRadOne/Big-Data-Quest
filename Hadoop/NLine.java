import java.util.*;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
public class NLine
{
   public static class Map extends Mapper <LongWritable, Text, LongWritable, Text>
   {
	   public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException 
	      { 
	   context.write(key,value);
	      }
   }
    public static void main(String args[])throws Exception
    {
       Configuration conf=new Configuration();
       conf.setInt(NLineInputFormat.LINES_PER_MAP, 3);
       Job job = new Job(conf,"NLine");
       job.setJarByClass(NLine.class);
       job.setOutputKeyClass(Text.class);
       job.setOutputValueClass(Text.class);
       job.setMapperClass(Map.class);
       job.setNumReduceTasks(0);
       job.setInputFormatClass(NLineInputFormat.class);
       job.setOutputFormatClass(TextOutputFormat.class);
       FileInputFormat.addInputPath(job,new Path(args[0]));
       FileOutputFormat.setOutputPath(job,new Path(args[1]));
       job.waitForCompletion(true);
    }
 }
