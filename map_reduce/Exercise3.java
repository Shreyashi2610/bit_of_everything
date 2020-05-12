import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class Exercise3 extends Configured implements Tool {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

	private Text myvalue=new Text(); 
	
	public void configure(JobConf job) {
	}

	// Can have multiple map functions for multuple inputs. You cannot use multiple map functions, however, without multiple inputs.
	// Input Key is always a LongWritable and input value is always Text - cannot change those
	// Can change OutputCollector data types, though typically the output key is always Text
	public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

	    String[] col = value.toString().split(","); // Converts text object line to a java String. 

	    String year = col[166];

		if(year.matches("^\\d{4}$") && Integer.parseInt(year) >= 2000 && Integer.parseInt(year) <= 2010) //Check if the last column is false. Notice, we didn't need to count the columns
      		{
      			Text groups = new Text(); //Initialize key
      			groups.set(col[1] + ", " + col[2] + ", " + col[3]); //This key will need to be legible, so I separate with commas
      			myvalue.set("");
      			output.collect(groups, myvalue); //collect the keys
      		}
	}
    }

    public int run(String[] args) throws Exception {
	JobConf conf = new JobConf(getConf(), Exercise3.class);
	conf.setJobName("exercise3"); 
	conf.setOutputKeyClass(Text.class);				// If your variable types between map and reduce are the same, these must match map output, reduce input, and reduce output
	conf.setOutputValueClass(Text.class);    // If they are different, these must match reduce output

	conf.setMapperClass(Map.class);					// DO NOT MAKE CHANGES HERE -- unclear if the CombinerClass line needs to be removed
	// conf.setMapperClass(Map2.class);				// If you have multiple map functions
	// conf.setCombinerClass(Reduce.class);			// Combiner is not in this file. See (https://canvas.northwestern.edu/courses/112171/files/folder/Lab%20Materials/Lab%202%3A%20MapReduce?preview=8596152) for combiner example
	// Comment combiner out if you do not have it
	//conf.setReducerClass(Reduce.class);

	conf.setInputFormat(TextInputFormat.class);		
	conf.setOutputFormat(TextOutputFormat.class);	

	FileInputFormat.setInputPaths(conf, new Path(args[0]));
	FileOutputFormat.setOutputPath(conf, new Path(args[1]));

	JobClient.runJob(conf);
	return 0;
    }

	public static void main(String[] args) throws Exception {
	int res = ToolRunner.run(new Configuration(), new Exercise3(), args);
	System.exit(res);
    }
}