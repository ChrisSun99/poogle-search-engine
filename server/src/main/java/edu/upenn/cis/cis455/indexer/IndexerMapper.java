package edu.upenn.cis.cis455.indexer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import edu.stanford.nlp.io.EncodingPrintWriter.out;
import edu.upenn.cis.cis455.crawler.utils.Stemmer;
import edu.upenn.cis.cis455.storage.dynamodb.DocumentData;
import edu.upenn.cis.cis455.storage.dynamodb.DynamoDBInterface;

/**
 * Indexer Mapper
 */
public class IndexerMapper extends Mapper<LongWritable, Text, Text, Text> {
	private final Text url = new Text();
	private final String splitOn = " ,.?!-[({\t\"\'\\_:;";
	private final DynamoDBInterface db = new DynamoDBInterface();
	

	@Override 
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		Text word = new Text();
		String DocId = value.toString().substring(0, value.toString().indexOf("\t"));
	    String doc =  value.toString().substring(value.toString().indexOf("\t") + 1);
	    System.out.println(DocId);
		if (doc != null) {
			String line = doc;
			String sanitizedUrl = DocId;
			if (sanitizedUrl.contains(" ")) {
				sanitizedUrl.replaceAll(" ", "%20");
			}
			url.set(sanitizedUrl);

			String rawContent = getHTMLText(line);
			StringTokenizer tokenizer = new StringTokenizer(rawContent,
					splitOn);
			while (tokenizer.hasMoreTokens()) {
				String currWord = tokenizer.nextToken();
				if ( currWord.length() > 50)
					continue;
				currWord = currWord.toLowerCase().replaceAll("[^a-z0-9]", "")
						.trim();
				if (!currWord.matches("[0-9]+") && !currWord.isEmpty() && !stopwords.contains(currWord)) {
					word.set(stem(currWord));
					context.write(word, url);
				}
			}
		}
	}
	
	/**
	 * Format the HTML text
	 * @param html
	 * @return textContent
	 */
	private String getHTMLText(String html) {
		html = html.replaceAll("<", " <");
		html = html.replaceAll(">", "> ");
		Document doc = Jsoup.parse(html.replaceAll("(?i)<br[^>]*>", "<pre>\n</pre>"));
		String textContent = doc.select("body").text();
		return textContent;
	}
	
	/**
	 * 
	 * @param word
	 * @return stemmedWord
	 */
	private String stem(String word) {
		Stemmer stemmer = new Stemmer();
		char[] charArray = word.toCharArray();
		stemmer.add(charArray, word.length());
		stemmer.stem();
		String stemmedWord = stemmer.toString();
		return stemmedWord;
	}
	
	// TODO: replace with completed stopwords
	public static ArrayList<String> stopwords = new ArrayList<String>(
			Arrays.asList(("a,about,above,"
					+ "after,again,against,all,am,an,and,any,are,"
					+ "aren't,as,at,be,because,been,before,being,"
					+ "below,between,both,but,by,could,"
					+ "couldn't,did,didn't,do,does,doesn't,doing,don't,"
					+ "down,during,each,few,for,from,further,had,hadn't,"
					+ "has,hasn't,have,haven't,having,he,he'd,he'll,he's,"
					+ "her,here,here's,hers,herself,him,himself,his,"
					+ "how's,i,i'd,i'll,i'm,i've,if,in,into,is,isn't,it,"
					+ "it's,its,itself,let's,me,more,mustn't,my,myself,"
					+ "no,nor,of,off,on,once,only,or,other,ought,our,ours,"
					+ "ourselves,out,over,own,shan't,she,she'd,she'll,"
					+ "she's,should,shouldn't,so,some,such,than,that,that's,"
					+ "the,their,theirs,them,themselves,then,there,there's,"
					+ "these,they,they'd,they'll,they're,they've,this,those,"
					+ "through,to,too,under,until,up,very,was,wasn't,we,we'd,"
					+ "we'll,we're,we've,were,weren't,what's,when's,"
					+ "where's,while,who's,why's,with,"
					+ "won't,would,wouldn't,you,you'd,you'll,you're,you've,your,"
					+ "yours,yourself,yourselves,").split(",")));
		
}

