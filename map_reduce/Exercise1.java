import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.lib.MultipleInputs;

public class Exercise1 extends Configured implements Tool {
	
	public static class Map1 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	
		public void configure(JobConf job) {
		}

		protected void setup(OutputCollector<Text, Text> output) throws IOException, InterruptedException {
    	}

		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		    
		    String line = value.toString(); // Line of text in data
		    String[] tokens = line.split("\\s+"); // Parsing the four fields
		    
		    String year = tokens[1];

		    String comb_key = new String(); //substring + year makes a key

		    
		    if (tokens[0].toUpperCase().contains("nu".toUpperCase())){

		    	comb_key = "nu" + " " + year; //"nu" + year

		    	} 

		    else if (tokens[0].toUpperCase().contains("chi".toUpperCase())){ 

		    	comb_key = "chi" + " " + year; //"chi" + year

		    	}

		    else if (tokens[0].toUpperCase().contains("haw".toUpperCase())){ 

		    	comb_key = "haw" + " " + year; //"haw" + year

		    	}
		    
		    if(year.matches("^\\d{4}$") && ((tokens[0].toUpperCase().contains("nu".toUpperCase())) || (tokens[0].toUpperCase().contains("chi".toUpperCase())) || (tokens[0].toUpperCase().contains("haw".toUpperCase())) )){
		        //checking for valid year value
		        output.collect(new Text(comb_key), new Text(tokens[3]+", 1")); //key -> "nu"/"chi"/"haw" + year
		        															   //value -> volume + count (constant 1 here)
		        
		        }
		}

		protected void cleanup(OutputCollector<Text, Text> output) throws IOException, InterruptedException {
		}
	}

	public static class Map2 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		//declare private variable for comparison 
	
		public void configure(JobConf job) {
		}

		protected void setup(OutputCollector<Text, Text> output) throws IOException, InterruptedException {
    	}

		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		    
		    String line = value.toString(); // Line of text in data
		    String[] tokens = line.split("\\s+"); // Parsing the four fields
		    
		    String year = tokens[2]; //year is 3rd field

		    int count = 0;

		    String comb_key = new String(); //substring + year makes a key

		    
		    if ((tokens[0].toUpperCase().contains("nu".toUpperCase())) || (tokens[1].toUpperCase().contains("nu".toUpperCase()))){ 

		    	int word1 = tokens[0].toUpperCase().contains("nu".toUpperCase()) ? 1 : 0; //if first word contains substring
		    	int word2 = tokens[1].toUpperCase().contains("nu".toUpperCase()) ? 1 : 0; //if second word contains substring

		    	count = count + word1 + word2; //here count in (1,2)

		    	comb_key = "nu" + " " + year; // key -> "nu" + year

		    	}

		    else if ((tokens[0].toUpperCase().contains("chi".toUpperCase())) || (tokens[1].toUpperCase().contains("chi".toUpperCase()))){ 

		    	int word1 = tokens[0].toUpperCase().contains("chi".toUpperCase()) ? 1 : 0;
		    	int word2 = tokens[1].toUpperCase().contains("chi".toUpperCase()) ? 1 : 0;

		    	count = count + word1 + word2; // here count in (1,2)
		    	
		    	comb_key = "chi" + " " + year; // key -> "chi" + year

		    	} 

		    else if ((tokens[0].toUpperCase().contains("haw".toUpperCase())) || (tokens[1].toUpperCase().contains("haw".toUpperCase()))){ 

		    	int word1 = tokens[0].toUpperCase().contains("haw".toUpperCase()) ? 1 : 0;
		    	int word2 = tokens[1].toUpperCase().contains("haw".toUpperCase()) ? 1 : 0;

		    	count = count + word1 + word2; // here count in (1,2)
		    	
		    	comb_key = "haw" + " " + year; // key -> "haw" + year

		    	}
		    
		    if(year.matches("^\\d{4}$") && (tokens[0].toUpperCase().contains("nu".toUpperCase()) || tokens[1].toUpperCase().contains("nu".toUpperCase()) ||
		    								tokens[0].toUpperCase().contains("chi".toUpperCase()) || tokens[1].toUpperCase().contains("chi".toUpperCase()) ||
		    								tokens[0].toUpperCase().contains("haw".toUpperCase()) || tokens[1].toUpperCase().contains("haw".toUpperCase()) )){ 
		        
		        output.collect(new Text(comb_key), new Text(tokens[3]+", "+count)); //key -> "nu"/"chi"/"haw" + year
		    																		//value -> volume + count (1,2)
		        
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
			
			double sum = 0.0, count = 0.0, avg = 0.0;
	    
 
	        while (values.hasNext()) {
				Text pair = values.next();
				String[] tokens = pair.toString().split(",");
				count = count + Double.parseDouble(tokens[1]);
			    sum = sum + Double.parseDouble(tokens[1]) * Double.parseDouble(tokens[0]); 
		    }

			avg = sum/count;
	        output.collect(key, new DoubleWritable(avg));
        }

        protected void cleanup(OutputCollector<Text, DoubleWritable> output) throws IOException, InterruptedException {
		}
	}

	public int run(String[] args) throws Exception {
		JobConf conf = new JobConf(getConf(), Exercise1.class);
		conf.setJobName("exercise1");

		conf.setOutputKeyClass(Text.class); 
		conf.setOutputValueClass(Text.class);

		// conf.setMapperClass(Map.class);
		conf.setMapperClass(Map1.class);
		conf.setMapperClass(Map2.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

	    MultipleInputs.addInputPath(conf, new Path(args[0]), TextInputFormat.class,Map1.class);
	    MultipleInputs.addInputPath(conf, new Path(args[1]), TextInputFormat.class,Map2.class);
	    FileOutputFormat.setOutputPath(conf, new Path(args[2]));

		JobClient.runJob(conf);
		return 0;
	}

    public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new Exercise1(), args);
		System.exit(res);
    }
}