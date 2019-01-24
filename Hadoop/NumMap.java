import java.util.*; 
import java.io.IOException;
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.conf.*; 
import org.apache.hadoop.io.*; 
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
public class NumMap
{
   public static class Map extends Mapper <LongWritable, Text, Text, IntWritable>
   {
	  int c=0;
	  IntWritable one=new IntWritable(1);
	  private Text word=new Text();
      public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException 
      { 
         String[] line = value.toString().split(",");
         for(String lines:line) 
         {
        	 c++;
             //context.write(new Text(lines),one);
          }  
      }
         public void cleanup(Context context) throws IOException, InterruptedException 
         {
      	  Text word = new Text("The number of words are:");
       	  context.write(new Text(word),new IntWritable(c));
         }
       } 
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> 
    {
       int num = 0;
       public void reduce(Text key,Iterable <IntWritable> values, Context context) throws IOException, InterruptedException 
       { 
          int sum=0;
          for(IntWritable val:values)
          {
         	 sum+=val.get();
          }
         num=num+sum;
       }
       @Override
       public void cleanup(Context context) throws IOException, InterruptedException 
       {
    	  Text word = new Text("The number of words are:");
     	  context.write(new Text(word),new IntWritable(num));
       }
       }
    public static void main(String args[])throws Exception 
    {
       Configuration conf=new Configuration();
       Job job = new Job(conf,"numwords");
       job.setJarByClass(NumWords.class); 
       job.setOutputKeyClass(Text.class);
       job.setOutputValueClass(IntWritable.class);
       job.setMapperClass(Map.class);
       job.setReducerClass(Reduce.class);
       job.setNumReduceTasks(0);
       job.setInputFormatClass(TextInputFormat.class);
       job.setOutputFormatClass(TextOutputFormat.class);
       FileInputFormat.addInputPath(job,new Path(args[0]));
       FileOutputFormat.setOutputPath(job,new Path(args[1]));
       job.waitForCompletion(true); 
    } 
 } 


