import java.io.*;
import java.util.*;
import java.math.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.lib.MultipleInputs;

public class Exercise4 extends Configured implements Tool {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

	private Text mykey = new Text(); 
	
	public void configure(JobConf job) {
	}

	protected void setup(OutputCollector<Text, DoubleWritable> output) throws IOException, InterruptedException {
    }

	// Can have multiple map functions for multuple inputs. You cannot use multiple map functions, however, without multiple inputs.
	// Input Key is always a LongWritable and input value is always Text - cannot change those
	// Can change OutputCollector data types, though typically the output key is always Text
	public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {

	    double duration;
	    String[] col = value.toString().split(","); // Converts text object line to a java String. 

	    mykey.set(col[2]);
      	duration = Double.parseDouble(col[3]);

		output.collect(mykey, new DoubleWritable(duration)); //collect the keys
	}

	protected void cleanup(OutputCollector<Text, DoubleWritable> output) throws IOException, InterruptedException {
		}
	}	

	public static class Partition implements Partitioner<Text, DoubleWritable> {
    public void configure(JobConf job) {
    }

    public int getPartition(Text key, DoubleWritable value, int numReduceTasks) {

        String mykey = key.toString();
        String first_char = mykey.substring(0,1);


        if(numReduceTasks == 0) {
            return 0;
        }

        if(first_char.compareTo("f") <= 0)
         {
            return 0;
         }
         else if(first_char.compareTo("k") <= 0)
         {
            return 1 % numReduceTasks;
         }
         else if(first_char.compareTo("p") <= 0)
         {
            return 2 % numReduceTasks;
         }
         else if(first_char.compareTo("u") <= 0)
         {
            return 3 % numReduceTasks;
         }
         else
         {
            return 4 % numReduceTasks;
         }
    }

    protected void cleanup(OutputCollector<Text, DoubleWritable> output) throws IOException, InterruptedException {
    }
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	    
		public void configure(JobConf job) {
		}
		
		protected void setup(OutputCollector<Text, DoubleWritable> output) throws IOException, InterruptedException {
    	}
		
		public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
			
			double max = 0.0;
	    
 
	        while (values.hasNext()){ // for each value in the list, add to the count and sum
				if (max < values.next().get()) {
					max = values.next().get();
				}
			}

			output.collect(key, new DoubleWritable(max));
        }

        protected void cleanup(OutputCollector<Text, DoubleWritable> output) throws IOException, InterruptedException {
		}
        
	}

 	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), Exercise4.class);
		conf.setJobName("exercise4");

		conf.setMapOutputKeyClass(Text.class); //map key
		conf.setMapOutputValueClass(DoubleWritable.class); //map value
		conf.setOutputKeyClass(Text.class); //reduce key
		conf.setOutputValueClass(DoubleWritable.class); //reduce value

		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		conf.setPartitionerClass(Partition.class);
		conf.setNumReduceTasks(5);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

	    FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
		return 0;
	}

    public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Exercise4(), args);
		System.exit(res);
    }
}
