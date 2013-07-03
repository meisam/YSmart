/*
        The following code is automatically generated by YSmart 12.01.
        Author: Rubao Lee, Yuan Yuan    
        Email: yuanyu@cse.ohio-state.edu
*/

package edu.osu.cse.ysmart.q2_2;
import java.io.IOException;
import java.util.*;
import java.text.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.partition.*;


public class q2_2004 extends Configured implements Tool{

	public static class Map extends  Mapper<Object, Text,IntWritable,Text>{

		private int left = 0;
		public void setup(Context context) throws IOException, InterruptedException {

			int last_index = -1, start_index = -1;
			String path = ((FileSplit)context.getInputSplit()).getPath().toString();
			last_index = path.lastIndexOf('/');
			last_index = last_index - 1;
			start_index = path.lastIndexOf('/',last_index);
			String f_name = path.substring(start_index+1,last_index+1);
			if(f_name.compareTo("q2_2005") == 0 )
				left = 1;
		}
		public void map(Object key, Text value,Context context) throws IOException,InterruptedException{

			String line = value.toString();
			int prev=0,i=0,n=0;
			if(this.left == 1){

				String[] line_buf = new String[4];
				for(i=0,n=0,prev=0;i<line.length();i++){

					if (line.charAt(i) == '|'){
						line_buf[n] = line.substring(prev,i);
						n = n+1;
						prev = i+1;
					}
					if(n == 4)
						break;
				}

			if(n<4)
				line_buf[n] = line.substring(prev,i);
				context.write(new IntWritable(Integer.parseInt(line_buf[3])), new Text("L"+"|" +line_buf[0]+ "|"+line_buf[1]+ "|"+line_buf[2]+ "|"+line_buf[3]+ "|"));
			}else{

				String[] line_buf = new String[5];
				for(i=0,n=0,prev=0;i<line.length();i++){

					if (line.charAt(i) == '|'){
						line_buf[n] = line.substring(prev,i);
						n = n+1;
						prev = i+1;
					}
					if(n == 5)
						break;
				}

			if(n<5)
				line_buf[n] = line.substring(prev,i);
				if(line_buf[4].compareTo("MFGR#2221") >= 0 && line_buf[4].compareTo("MFGR#2228") <= 0){

					context.write(new IntWritable(Integer.parseInt(line_buf[0])), new Text("R"+"|" +line_buf[4]+ "|" +Integer.parseInt(line_buf[0])+ "|" ));
				}
			}

		}

	}

	public static class Reduce extends  Reducer<IntWritable,Text,NullWritable,Text>{

		public void reduce(IntWritable key, Iterable<Text> v, Context context) throws IOException,InterruptedException{

			Iterator values = v.iterator();
			ArrayList al_left = new ArrayList();
			ArrayList al_right = new ArrayList();
			while(values.hasNext()){

				String tmp = values.next().toString();
				if(tmp.charAt(0) == 'L'){

					al_left.add(tmp.substring(2));
				}else{

					al_right.add(tmp.substring(2));
				}

			}

			NullWritable key_op = NullWritable.get();
			for(int i=0;i<al_left.size();i++){

				String[] left_buf = ((String)al_left.get(i)).split("\\|");
				for(int j=0;j<al_right.size();j++){

					String[] right_buf = ((String)al_right.get(j)).split("\\|");
					context.write(key_op, new Text(Integer.parseInt(left_buf[0])+ "|" +right_buf[0]+ "|" +Double.parseDouble(left_buf[1])+ "|" +Integer.parseInt(left_buf[2])+ "|" ));
				}

			}

		}

	}

	public int run(String[] args) throws Exception{

		Configuration conf = new Configuration();
		Job job = new Job(conf,"q2_2004");
		job.setJarByClass(q2_2004.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileInputFormat.addInputPath(job,new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		return (job.waitForCompletion(true) ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {

			int res = ToolRunner.run(new Configuration(), new q2_2004(), args);
			System.exit(res);
	}

}
