import java.util.*; 
import java.io.IOException;
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.conf.*; 
import org.apache.hadoop.io.*; 
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
public class MultipleClass 
{
	public static class Map extends Mapper<LongWritable,Text,Text,IntWritable>
	{
	    public void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException	
	    {
		String[] line=value.toString().split(",");
		for(String lines:line) 
        {
            context.write(new Text(lines),new IntWritable(1));
         } 	
	    }
	}
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> 
    {
	public MultipleOutputs  multipleOutputs;
		 
	public void setup(Context context) throws IOException, InterruptedException
	{
					
		          multipleOutputs = new MultipleOutputs(context);
	}
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
	{
			        int sum = 0;
			        for (IntWritable val : values) 
			        {
			            sum += val.get();
			        }
			       multipleOutputs.write(key, new IntWritable(sum),key.toString());
     }
	public void cleanup(Context context) throws IOException, InterruptedException
	{
				      multipleOutputs.close();
	}
			   
	}
	public static void main(String[] args) throws Exception 
	{
			Configuration conf = new Configuration();
			Job job = new Job(conf, "wordcount");
			job.setJarByClass(MultipleClass.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);
			    
			job.setInputFormatClass(TextInputFormat.class);
			 
			LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			 // MultipleOutputs.addNamedOutput(job, "wordcount", TextOutputFormat.class, Text.class, IntWritable.class);
			job.waitForCompletion(true);
			
	}        

}


