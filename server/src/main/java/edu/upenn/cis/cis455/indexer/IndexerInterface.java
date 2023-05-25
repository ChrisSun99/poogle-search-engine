package edu.upenn.cis.cis455.indexer;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;

import edu.upenn.cis.cis455.storage.dynamodb.DynamoDBFactory;
import edu.upenn.cis.cis455.storage.dynamodb.DynamoDBInterface;
import edu.upenn.cis.cis455.storage.dynamodb.InvertedIndexData;

/**
 * Indexer main function
 *
 */
public class IndexerInterface extends Configured implements Tool {
	static AmazonDynamoDB client = DynamoDBFactory.createClient();
    static DynamoDB db = new DynamoDB(client);
    static DynamoDBInterface dbInterface = new DynamoDBInterface();
	
	public static void main(String[] args) throws Exception {
//		int res = ToolRunner.run(new Configuration(), new IndexerInterface(), args);
//        System.exit(res);
      Path file = new Path("reducerOutput-4.txt");
      System.out.println("Starting the upload");

      Configuration conf = new Configuration();
      List<InvertedIndexData> res = new ArrayList<>();
      
      org.apache.hadoop.fs.FileSystem fs;
      LineIterator iter;
		try {
			fs = file.getFileSystem(conf);
			iter = IOUtils.lineIterator(fs.open(file), "UTF8");
			int i = 0;
	        while (iter.hasNext()) {
		         String line = iter.nextLine();
		         String[] parts = line.split("\t");
//		         System.out.println(parts.length);

		         System.out.println(parts[0] + " " + parts[1] + " " + parts[2] + " " + parts[3]);
		         
	        	 InvertedIndexData item = new InvertedIndexData();
			     item.setWord(parts[0]);
			     item.setMd5(parts[1]);
			     item.setIdf(Float.parseFloat(parts[2]));
			     item.setTfidf(Float.parseFloat(parts[3]));
//			     db.updateInvertedIndexData(item);
			     res.add(item); 
			     i++;
			     if (i % 10000 == 0) {
			    	 System.out.println("============Uploading to DB============"+file.getName());
			    	 dbInterface.batchUpdateInvertedIndexData(res);
			    	 i = 0; 
			    	 res = new ArrayList<>();
			     }
	       }
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		dbInterface.batchUpdateInvertedIndexData(res);
    }

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String inputPath = args[0];
		String outputPath = args[1];
		
		Path output = new Path(outputPath);
//		createInputFile(inputPath);
//		output.getFileSystem(conf).delete(output, true);
//		System.out.println(output.toString());
//		output.getFileSystem(conf).mkdirs(output);
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
	
	private static String getHTMLText(String html) {
		html = html.replaceAll("<", " <");
		html = html.replaceAll(">", "> ");
		Document doc = Jsoup.parse(html.replaceAll("(?i)<br[^>]*>", "<pre>\n</pre>"));
		String textContent = doc.select("body").text();
		return textContent;
	}
	
	private void createInputFile(String input) {
      ScanSpec scanSpec = new ScanSpec().withAttributesToGet("md5", "document");

      db.getTable("DOCUMENT-DEMO").scan(scanSpec).pages().forEach(page -> {
      page.forEach(item -> {
        try {
        	String filename= input;
            FileWriter fw = new FileWriter(filename,true); //the true will append the new data
            fw.write(item.getString("md5") + "\t");//appends the string to the file
            String formattedDoc = item.getString("document");
            String rawContent = getHTMLText(formattedDoc);
            
            fw.write(rawContent + "\t");
            fw.write("\n");
            fw.close();
        } catch (IOException e) {
      	  e.printStackTrace();
        }
       }); 
      });
	}
}
