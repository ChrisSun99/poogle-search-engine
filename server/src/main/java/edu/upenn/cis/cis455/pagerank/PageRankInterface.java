package edu.upenn.cis.cis455.pagerank;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Interface for launching Page Rank calculation
 */
public class PageRankInterface {
	
	/**
	 * Launch the Page Rank job based on the input file and store it to the desired output directory
	 * Input:
	 * 1. input: input file location
	 * 2. output: output directory location
	 * 3. ifPenalizeIntradomain: true if we want to distribute less weight to intra-domain links, false if we want to treat intra-domain and inter-domain links the same
	 */
	public static void calculatePageRank(String input, String output, boolean ifPenalizeIntradomain) throws Exception {
		Configuration conf = new Configuration();
	    
	    Path outputPath = new Path(output);
	    outputPath.getFileSystem(conf).delete(outputPath, true);
	    outputPath.getFileSystem(conf).mkdirs(outputPath);
 
	    Path inputPath = new Path(outputPath, "input.txt");
	    int numNodes = createInputFile(new Path(input), inputPath);
	    
	    int ite = 1;
	    double desiredConvergence = 0.01;
	    Path jobOutputPath = new Path(outputPath, String.valueOf(ite));
	    
	    // iterate the map reduce job until reaching convergence
	    while (iteratePageRank(inputPath, jobOutputPath, numNodes, ifPenalizeIntradomain) >= desiredConvergence) {
	    	inputPath = new Path(outputPath, String.valueOf(ite));
	    	ite++;
	    	jobOutputPath = new Path(outputPath, String.valueOf(ite));
	    }
	    
	    // store the output of map reduce job
	    createOutputFile(new Path(jobOutputPath, "part-r-00000"), new Path(outputPath, "output.txt"));
	}
	
	/**
	 * Launch one Page Rank iteration job
	 * Input:
	 * 1. inputPath: input file location for map reduce job
	 * 2. outputPath: output directory location for map reduce job
	 * 3. numNodes: number of urls in total
	 * 4. ifPenalizeIntradomain: true if we want to distribute less weight to intra-domain links, false if we want to treat intra-domain and inter-domain links the same
	 * Output:
	 * Convergence of the algorithm (double)
	 */
	public static double iteratePageRank(Path inputPath, Path outputPath, int numNodes, boolean ifPenalizeIntradomain) throws Exception {
		Configuration conf = new Configuration();
	    
	    Job job = Job.getInstance(conf, "PageRankJob");
	    job.setJarByClass(PageRankInterface.class);
	    if (ifPenalizeIntradomain) {
	    	job.setMapperClass(MapPenalizeIntradomain.class);
	    } else {
	    	job.setMapperClass(Map.class);
	    }
	    job.setReducerClass(Reduce.class);
 
	    job.setInputFormatClass(KeyValueTextInputFormat.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
 
	    FileInputFormat.setInputPaths(job, inputPath);
	    FileOutputFormat.setOutputPath(job, outputPath);
 
	    if (!job.waitForCompletion(true)) {
	      throw new Exception("Job failed");
	    }
 
	    long summedConvergence = job.getCounters().findCounter(Reduce.Counter.CONV_DELTAS).getValue();
	    double convergence = (double) summedConvergence / (double) Reduce.CONVERGENCE_SCALING_FACTOR / (double) numNodes;
 
	    System.out.println("======================================");
	    System.out.println("=  Num nodes:           " + numNodes);
	    System.out.println("=  Summed convergence:  " + summedConvergence);
	    System.out.println("=  Convergence:         " + convergence);
	    System.out.println("======================================");
 
	    return convergence;
	}
	
	/**
	 * Format the input file into the desired style for map reduce job
	 * Input:
	 * 1. file: input file location
	 * 2. output: desired map reduce job input file location
	 * Output:
	 * Number of urls in total
	 */
	public static int createInputFile(Path file, Path targetFile)
	      throws IOException {
	    Configuration conf = new Configuration();
	    
	    FileSystem fs = file.getFileSystem(conf);
 
	    int numNodes = getNumNodes(file);
	    double initialPageRank = 1.0;
 
	    LineIterator iter = IOUtils.lineIterator(fs.open(file), "UTF8");
	    
	    Set<String> crawledUrls = new HashSet<String> ();
	    
	    while (iter.hasNext()) {
	      String line = iter.nextLine();
 
	      String[] parts = StringUtils.split(line);
 
	      crawledUrls.add(parts[0]);
	    }
	    
	    OutputStream os = fs.create(targetFile);
	    iter = IOUtils.lineIterator(fs.open(file), "UTF8");
 
	    while (iter.hasNext()) {
	      String line = iter.nextLine();
 
	      String[] parts = StringUtils.split(line);
 
	      Node node = new Node();
	      node.setValue(initialPageRank);
	      List<String> outboundLink = new ArrayList<String>();
	      for (int i = 1; i < parts.length; i++) {
	    	  if (crawledUrls.contains(parts[i]) && !parts[0].equals(parts[i])) {
	    		  outboundLink.add(parts[i]);
	    	  }
	      }
	      String[] outboundLinkArray = outboundLink.toArray(new String[0]);
	      node.setAdjacentNodes(outboundLinkArray);
	      node.setNode(true);
	      IOUtils.write(parts[0] + '\t' + node.toString() + '\n', os);
	    }
	    os.close();
	    return numNodes;
	  }
	
	/**
	 * Write the map reduce job results to the desired location
	 * Input:
	 * 1. file: output file location of map reduce job
	 * 2. output: desired output file location
	 */
	public static void createOutputFile(Path file, Path targetFile)
		      throws IOException {
		    Configuration conf = new Configuration();
		    
		    FileSystem fs = file.getFileSystem(conf);

		    OutputStream os = fs.create(targetFile);
		    LineIterator iter = IOUtils.lineIterator(fs.open(file), "UTF8");
	 
		    while (iter.hasNext()) {
		      String line = iter.nextLine();
	 
		      String[] parts = StringUtils.split(line);

		      IOUtils.write(parts[0] + '\t' + parts[1] + '\n', os);
		    }
		    os.close();
	}
	
	/**
	 * Get the number of urls as keys from the file
	 * Input:
	 * 1. file: input file location
	 * Output:
	 * Number of urls in total
	 */
	public static int getNumNodes(Path file) throws IOException {
	    Configuration conf = new Configuration();
	    
	    FileSystem fs = file.getFileSystem(conf);
 
	    return IOUtils.readLines(fs.open(file), "UTF8").size();
	}
	
	/**
	 * Input:
	 * 1. input file location
	 * 2. output directory location
	 * 3. true if we want to distribute less weight to intra-domain links, false if we want to treat intra-domain and inter-domain links the same
	 */
	public static void main( String[] args) {
		String inputFile = args[0];
		String outputDir = args[1];
		boolean ifPenalizeIntradomain = Boolean.valueOf(args[2]);
		
		try {
			calculatePageRank(inputFile, outputDir, ifPenalizeIntradomain);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
	
}

