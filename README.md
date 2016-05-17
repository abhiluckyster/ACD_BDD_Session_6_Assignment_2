# ACD_BDD_Session_6_Assignment_2
Find the total number of medals won by India Year vise.

package IndiaMedals;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class IndiaMedalsMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException{
		String[] values = value.toString().split("\t");
			if(values[2].compareToIgnoreCase("India")==0){
				context.write(new Text(values[3]), new Text(values[9]));
			}
	}
}

package IndiaMedals;

import java.io.IOException;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

public class IndiaMedalsReducer extends Reducer<Text, Text, Text, IntWritable>{

	@Override
	protected void reduce(Text key, Iterable<Text> value, Context context)
	throws IOException, InterruptedException{
	int sum=0;
	for(Text values : value){
	String year = values.toString();
	int total= Integer.parseInt(year);
	sum=sum+total;
	}
	context.write(new Text(key), new IntWritable(sum));
	}
}

package IndiaMedals;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

public class IndiaMedalsDriver extends Configured implements Tool{
	
	public static void main (String abhi[]) 
			throws Exception{
		ToolRunner.run(new Configuration(), new IndiaMedalsDriver(), abhi);
	}
	
	public int run(String abhi[])throws Exception{
		
		Job job = Job.getInstance();
		job.setJobName("Abhinav - Medals win By India");
		job.setJarByClass(IndiaMedalsDriver.class);
		
		job.setMapperClass(IndiaMedalsMapper.class);
		job.setReducerClass(IndiaMedalsReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(abhi[0]));
		FileOutputFormat.setOutputPath(job, new Path(abhi[1]));
		
		job.waitForCompletion(true);
		return 0;
	}

}


