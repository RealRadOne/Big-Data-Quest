import java.util.*; 
import java.io.IOException;
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.conf.*; 
import org.apache.hadoop.io.*; 
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
public class One
{
   public static class Map extends Mapper <LongWritable, Text, Text, IntWritable>
   {
	  IntWritable one=new IntWritable(1);
	  private Text word=new Text();
	  int c=0;
      public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException 
      { 
         String[] line = value.toString().split(",");
         int G1=Integer.parseInt(line[8]);
         int G2=Integer.parseInt(line[9]);
         if(G1>10&&G2>8)
         {
        	 context.write(new Text(line[0] +" "+line[8]+" "),new IntWritable(G2));
        	 c=c+1;
         }
       }
      @Override
      public void cleanup(Context context) throws IOException, InterruptedException 
      {
    	  context.write(new Text("The count is:"), new IntWritable(c));
      }
    }
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> 
    {
       int max = 0;
	   Text word = new Text("");
       public void reduce(Text key,Iterable <IntWritable> values, Context context) throws IOException, InterruptedException 
       { 
          int sum=0;
          for(IntWritable val:values)
          {
         	 sum+=val.get();
          }
          if(max<sum)
         {
           max=sum;word.set(key);
         }
       }
     }
    public static void main(String args[])throws Exception 
    {
       Configuration conf=new Configuration();
       Job job = new Job(conf,"One");
       job.setJarByClass(One.class); 
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
