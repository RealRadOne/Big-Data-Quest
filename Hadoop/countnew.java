import java.util.*; 
import java.io.IOException;
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.conf.*; 
import org.apache.hadoop.io.*; 
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
public class countnew{
	public enum ct
	  {
		  cnt,nt
	  };
public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> 
{
 public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
   {
		          String[] line=value.toString().split(",");
		          int g1=Integer.parseInt(line[2]);
		          if(g1<10)
		          {
		          context.getCounter(ct.cnt).increment(1);
		          context.write(new Text(line[0]+" "+line[1]), new IntWritable(g1));
		          }
		          int average=Integer.parseInt(line[4]);
		          if(average > 13)
		          {
				      context.getCounter(ct.nt).increment(1);
				  }
  }
}  
public static void main(String args[])throws Exception 
{
   Configuration conf=new Configuration();
   Job job = new Job(conf,"countnew");
   job.setJarByClass(countnew.class); 
   job.setOutputKeyClass(Text.class);
   job.setOutputValueClass(Text.class);
   job.setMapperClass(Map.class);
   job.setNumReduceTasks(0);
   job.setInputFormatClass(TextInputFormat.class);
   job.setOutputFormatClass(TextOutputFormat.class);
   FileInputFormat.addInputPath(job,new Path(args[0]));
   FileOutputFormat.setOutputPath(job,new Path(args[1]));
   job.waitForCompletion(true); 
   Counters cn=job.getCounters();
   System.out.println("The students with Grade1 less than 10:"+cn.findCounter(ct.cnt).getValue());
   System.out.println("The students with average grade greater than 13:"+cn.findCounter(ct.nt).getValue());
} 
}
