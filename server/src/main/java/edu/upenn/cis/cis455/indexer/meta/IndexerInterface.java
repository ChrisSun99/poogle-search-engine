package edu.upenn.cis.cis455.indexer.meta;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.upenn.cis.cis455.indexer.meta.IndexerInterface;
import edu.upenn.cis.cis455.indexer.meta.IndexerMapper;
import edu.upenn.cis.cis455.indexer.meta.IndexerReducer;

public class IndexerInterface extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new IndexerInterface(), args);
        System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		String inputPath = args[0];
		String outputPath = args[1];
		Path output = new Path(outputPath);
		Job job = Job.getInstance(conf,IndexerInterface.class.getCanonicalName());
	    job.setJarByClass(IndexerInterface.class);
	    
	    job.setMapperClass(IndexerMapper.class);
	    job.setReducerClass(IndexerReducer.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    // For local testing
	    FileInputFormat.addInputPath(job, new Path(inputPath));
	    job.setInputFormatClass(TextInputFormat.class);
	    FileOutputFormat.setOutputPath(job, new Path(outputPath));
	    job.setOutputFormatClass(TextOutputFormat.class);
	    
	    return job.waitForCompletion(true) ? 0 : 1;
	}
}
