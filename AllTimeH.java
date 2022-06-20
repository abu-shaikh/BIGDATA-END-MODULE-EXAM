import java.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class AllTimeH {
	public static class MapClass extends Mapper <LongWritable,Text,Text,DoubleWritable>{
		private Text stock_id=new Text();
		private DoubleWritable High = new DoubleWritable();
		public void map(LongWritable key,Text value, Context context)
		{
			try {
				String[] str=value.toString().split(",");
				double high=Double.parseDouble(str[4]);
				stock_id.set(str[1]);
				High.set(high);
				context.write(stock_id,High);
			}
			catch(Exception e){
				System.out.println(e.getMessage());
			}
		}
	}
	
	public static class ReduceClass extends Reducer <Text,DoubleWritable,Text,DoubleWritable>{
		private DoubleWritable result=new DoubleWritable();
		
		public void reduce(Text key,Iterable<DoubleWritable> values,Context context) throws IOException,InterruptedException{
			double max=0;
			double temp=0;
			
			for (DoubleWritable value:values) {
				temp=value.get();
				if (temp>max) {
					max=temp;
				}
			}
			result.set(max);
			context.write(key, result);
		}
	}
	
	public static void main (String[] args) throws Exception{
		Configuration conf=new Configuration();
		Job job=Job.getInstance(conf,"Highest Price for each stock");
		job.setJarByClass(AllTimeH.class);
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		System.exit(job.waitForCompletion(true)?0:1);
	}

}
