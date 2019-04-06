import java.util.*; 
import java.io.IOException;
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.conf.*; 
import org.apache.hadoop.io.*; 
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
public class inviindex
{
		public static class Map extends Mapper<LongWritable,Text,Text,Text> 
		{ 
		public void map(LongWritable key, Text value, Context context)throws IOException,InterruptedException	 
				{
				 String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
				 String[] words = value.toString().split(" ");
				 int pos = 0;
				 for(String s:words)
				 {
				 	context.write(new Text(s), new Text(fileName+","+Integer.toString(pos)));
				 	pos++;
				 }
				 }
				 }
	    public static class Reduce extends Reducer<Text, Text, Text, Text> 
		{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		    {
			HashMap m=new HashMap();
			HashMap m1=new HashMap();
			int count=0;
			for(Text t:values)
			{
				String[] str=t.toString().split(",");	 
			if(m!=null &&m.get(str[0])!=null)
			{
			count=(int)m.get(str[0]); 	 
			m.put(str[0], ++count);
			m1.put(str[0],str[1]);
			}
			else
			{
			 		 	m.put(str[0], 1);
			 		 	m1.put(str[0],str[1]);
			}
			}
			context.write(key, new Text(m.toString()));
			context.write(key,new Text(m1.toString()));
			
			}
		}
			 
    public static void main(String args[])throws Exception 
    {
       Configuration conf=new Configuration();
       Job job = new Job(conf,"numwords");
       job.setJarByClass(inviindex.class); 
       job.setOutputKeyClass(Text.class);
       job.setOutputValueClass(Text.class);
       job.setMapperClass(Map.class);
       job.setReducerClass(Reduce.class);
       job.setInputFormatClass(TextInputFormat.class);
       job.setOutputFormatClass(TextOutputFormat.class);
       FileInputFormat.addInputPath(job,new Path(args[0]));
       FileOutputFormat.setOutputPath(job,new Path(args[1]));
       job.waitForCompletion(true); 
    } 
}
  
