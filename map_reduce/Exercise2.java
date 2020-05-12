import java.io.*;
import java.util.*;
import java.math.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.lib.MultipleInputs;

public class Exercise2 extends Configured implements Tool {
	
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	
		public void configure(JobConf job) {
		}

		protected void setup(OutputCollector<Text, Text> output) throws IOException, InterruptedException {
    	}

		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		    
		    String line = value.toString(); // Line of text in data
		    String[] tokens = line.split("\\s+"); // Parsing the four fields
		    String year = tokens[1];
		    String Key = " ";
		    
		    if(year.matches("^\\d{4}$")){ //checking for valid year value

		    	int square = Integer.parseInt(tokens[3])*Integer.parseInt(tokens[3]); //to store x-square
		        output.collect(new Text(Key), new Text(tokens[3] + ", " + square + ", 1")); //key -> 1 for all //value -> volume, volume square, count
		        
		        }
		}

		protected void cleanup(OutputCollector<Text, Text> output) throws IOException, InterruptedException {
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, DoubleWritable> {
	    
		public void configure(JobConf job) {
		}
		
		protected void setup(OutputCollector<Text, DoubleWritable> output) throws IOException, InterruptedException {
    	}
		
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
			
			double sum_sq = 0.0, sum = 0.0, count = 0.0, sd = 0.0;
	    
 
	        while (values.hasNext()) {
				Text pair = values.next();
				String[] tokens = pair.toString().split(",");

				count = count + Double.parseDouble(tokens[2]);
			    sum_sq = sum_sq + Double.parseDouble(tokens[1]); 
			    sum = sum + Double.parseDouble(tokens[0]);
		    }

			sd = Math.sqrt(sum_sq/count - Math.pow(sum/count,2));
	        output.collect(key, new DoubleWritable(sd));
        }

        protected void cleanup(OutputCollector<Text, DoubleWritable> output) throws IOException, InterruptedException {
		}
	}

	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), Exercise2.class);
		conf.setJobName("exercise2");

		conf.setMapOutputKeyClass(Text.class); //map key
		conf.setMapOutputValueClass(Text.class); //map value
		conf.setOutputKeyClass(Text.class); //reduce key
		conf.setOutputValueClass(DoubleWritable.class); //reduce value

		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

	    FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
		return 0;
	}

    public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Exercise2(), args);
		System.exit(res);
    }
}