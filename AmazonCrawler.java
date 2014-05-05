import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;

import org.jsoup.HttpStatusException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.net.SocketTimeoutException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.TimeUnit;

public class AmazonCrawler {

    public static class Map1 extends Mapper<LongWritable, Text, Text, IntWritable> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    String line = value.toString();
	    Text id = new Text();

	    if (line.contains("userId: ")) {
		id.set("U" + line.substring("review/userId: ".length()));
	    } else if (line.contains("productId: ")) {
		id.set("P" + line.substring("product/productId: ".length()));
	    }

	    if (line.length() > 3)
		context.write(id, new IntWritable(0));
	}
    }

    public static class Map2 extends Mapper<LongWritable, Text, Text, IntWritable> {

	private static int iteration;

	public void setup(Context context) throws IOException, InterruptedException {
	    Configuration conf = new Configuration();
	    FileSystem fs = FileSystem.get(conf);

	    FSDataInputStream in = fs.open(new Path("counter.txt"));
            iteration = in.readInt();
            in.close();

	    System.out.println("Iteration: " + iteration);
        }

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    String line = value.toString();
	    Text id = new Text();

	    String[] splitLine = line.split("\t");

	    if (splitLine.length > 1) {
		id.set(splitLine[0]);
		int iter = Integer.parseInt(splitLine[1]);
		context.write(id, new IntWritable(iter));
		
		if (iter == iteration) {
		    if (splitLine[0].length() > 1) {
			CrawlerThread crawler = new CrawlerThread(splitLine[0], iteration, context);
			crawler.run();

			crawler.join();
	    	    
		    }
		} else {
		    //System.out.println(" ** Skipping " + splitLine[0]);
		}
	    } 
	       
	}

    }

    /*
     *
     *
     */

    public static class Reduce1 extends Reducer<Text, IntWritable, Text, IntWritable> {
	private final static IntWritable zero = new IntWritable(0);

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
	    throws IOException, InterruptedException {
	    context.write(key, zero);
	}

	public void cleanup(Context context) throws IOException, InterruptedException {
	    Configuration conf = new Configuration();
	    FileSystem fs = FileSystem.get(conf);

	    FSDataOutputStream out = fs.create(new Path("counter.txt"));
	    out.writeInt(0);
	    out.close();
	}
    }

    public static class Reduce2 extends Reducer<Text, IntWritable, Text, IntWritable> {
	private static int iteration;

	private int duplicates = 0;
	private int total = 0;

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
	    throws IOException, InterruptedException {
	    int min = -1;

	    for (IntWritable value : values) {
		duplicates++;
		if (min < 0)
		    min = value.get();

		if (value.get() < min) {
		    min = value.get();
		}
	    }

	    total++;

	    context.write(key, new IntWritable(min));
	}

	public void cleanup(Context context) throws IOException, InterruptedException {
	    Configuration conf = new Configuration();
	    FileSystem fs = FileSystem.get(conf);

	    FSDataInputStream in = fs.open(new Path("counter.txt"));
	    iteration = in.readInt();
	    in.close();

	    System.out.println("Read : " + iteration);
	    iteration++;
	    System.out.println("Wrote : " + iteration);

	    FSDataOutputStream out = fs.create(new Path("counter.txt"));
	    out.writeInt(iteration);
	    out.close();

	    System.out.println("Duplicates: " + duplicates);
	    System.out.println("Total: " + total);
	    System.out.println("Ratio: " + total / duplicates);
	}
    }

    public static void main(String[] args) throws Exception {
	int threadCount = 26;
	
	String input = args[0];
	String output = "/output0";
	String user = args[1];
        int count = Integer.parseInt(args[2]);

	Configuration conf = new Configuration();
	FileSystem fs = FileSystem.get(conf);

	FSDataOutputStream out = fs.create(new Path("counter.txt"));
	out.writeInt(0);
	out.close();

	job1(conf, input, output);
	fs.copyToLocalFile(new Path(output + "/part-r-00000"), new Path("/home/" + user + "/amazon/output0"));
	
	input = "/output0/part-r-00000";
	for (int i = 0; i < count; i++) {
	    output = "/output" + (i + 1);

	    System.out.println("     %%%%%%% Iteration " + (i + 1));
	    System.out.println("  >> Input : " + input);
	    System.out.println("  >> Output : " + output);

	    job2(conf, input, output);
	    input = output + "/part-r-00000";
	    fs.copyToLocalFile(new Path(input), new Path("/home/" + user + "/amazon/output" + (i + 1)));
	}
    }

    public static void job1(Configuration conf, String input, String output) throws Exception {
	//Configuration conf = new Configuration();
	FileSystem fs = FileSystem.get(conf);

	if (!fs.exists(new Path("counter.txt"))) {
	    FSDataOutputStream out = fs.create(new Path("counter.txt"));
	    out.writeInt(0);
	    out.close();
	}

	Job job = new Job(conf, "amazon_job1");
	job.setJarByClass(AmazonCrawler.class);

	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);

	job.setMapperClass(Map1.class);
	job.setReducerClass(Reduce1.class);

	job.setInputFormatClass(TextInputFormat.class);
	job.setOutputFormatClass(TextOutputFormat.class);

	FileInputFormat.addInputPath(job, new Path(input));
	FileOutputFormat.setOutputPath(job, new Path(output));

	job.waitForCompletion(true);
    }

    public static void job2(Configuration conf, String input, String output) throws Exception {
        FileSystem fs = FileSystem.get(conf);

        Job job = new Job(conf, "amazon_job2");
        job.setJarByClass(Amazon.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map2.class);
        job.setReducerClass(Reduce2.class);

        job.setInputFormatClass(TextInputFormat.class);
	job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(input));
	FileInputFormat.setMaxInputSplitSize(job, 500);
        FileOutputFormat.setOutputPath(job, new Path(output));

	job.waitForCompletion(true);
    }

}

