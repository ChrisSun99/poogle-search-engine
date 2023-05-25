package edu.upenn.cis.cis455.storage.dynamodb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.io.FileSystem;
import org.apache.commons.io.LineIterator;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.TableKeysAndAttributes;
import com.amazonaws.services.dynamodbv2.document.spec.BatchGetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ExecuteStatementRequest;
import com.amazonaws.services.dynamodbv2.model.ExecuteStatementResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;

/**
 * Work with database
 *
 */
public class DynamoDBInterface {
    static Logger logger = LogManager.getLogger(DynamoDBInterface.class);

    AmazonDynamoDB client;
    DynamoDBMapper mapper;
    DynamoDB db;

    final static String URL_TABLE = "\"URL-DEMO\"";
    final static String DOCUMENT_TABLE = "DOCUMENT-DEMO";
    final static String INVERTED_INDEX_TABLE = "INVIDX-DEMO";

    final ObjectMapper om = new ObjectMapper();

    public DynamoDBInterface() {
        client = DynamoDBFactory.createClient();
        mapper = new DynamoDBMapper(client);
        db = new DynamoDB(client);
    }

    // C --------------------------------------------------
    private <T> void addData(T item) {
        DynamoDBMapperConfig dynamoDBMapperConfig = new DynamoDBMapperConfig.Builder()
                .withConsistentReads(DynamoDBMapperConfig.ConsistentReads.CONSISTENT)
                .withSaveBehavior(DynamoDBMapperConfig.SaveBehavior.PUT)
                .build();
        mapper.save(item, dynamoDBMapperConfig);
    }

    private <T> void batchAddData(Iterable<T> items) {
        DynamoDBMapperConfig dynamoDBMapperConfig = new DynamoDBMapperConfig.Builder()
                .withConsistentReads(DynamoDBMapperConfig.ConsistentReads.CONSISTENT)
                .withSaveBehavior(DynamoDBMapperConfig.SaveBehavior.PUT)
                .build();
        mapper.batchSave(items, dynamoDBMapperConfig);
    }

    public void addUrlData(UrlData item) {
        addData(item);
        logger.info("Added to " + URL_TABLE);
    }

    public void batchAddUrlData(Iterable<UrlData> items) {
        batchAddData(items);
        logger.info("Added to " + URL_TABLE);
    }

    public void addDocumentData(DocumentData item) {
        addData(item);
        logger.info("Added to " + DOCUMENT_TABLE);
    }

    public void batchAddDocumentData(Iterable<DocumentData> items) {
        batchAddData(items);
        logger.info("Added to " + DOCUMENT_TABLE);
    }

    public void addInvertedIndexData(InvertedIndexData item) {
        addData(item);
        logger.info("Added to " + INVERTED_INDEX_TABLE);
    }

    public void batchAddInvertedIndexData(Iterable<InvertedIndexData> items) {
        batchAddData(items);
        logger.info("Added to " + INVERTED_INDEX_TABLE);
    }

    // R --------------------------------------------------
    public UrlData getUrlData(String url) {
        UrlData item = mapper.load(UrlData.class, url);
        logger.info("Retrived from " + URL_TABLE);
        return item;
    }

    public List<UrlData> batchGetUrlData(Iterable<String> urls) {

        TableKeysAndAttributes hashKeys = new TableKeysAndAttributes(URL_TABLE);
        for (String url : urls) {
            hashKeys = hashKeys.addHashOnlyPrimaryKey("url", url);
        }
        BatchGetItemSpec spec = new BatchGetItemSpec().withTableKeyAndAttributes(hashKeys);

        List<UrlData> results = new ArrayList<>();
        db.batchGetItem(spec).getTableItems().get(URL_TABLE).forEach(item -> {
            try {
                results.add(om.readValue(item.toJSON(), UrlData.class));
            } catch (JsonProcessingException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        });

        return results;
    }
    
    public ExecuteStatementRequest getUrlWithHighestPageRankScoreRequest(String md5) {
        // PartiQL
        return new ExecuteStatementRequest()
                .withStatement(String.format(
                        "SELECT * FROM %s.\"md5-weight-index\" WHERE md5 = '%s' ORDER BY weight DESC",
                        URL_TABLE, md5))
                .withLimit(1);
    }
    
    public ExecuteStatementResult getUrlWithHighestPageRankScoreResult(ExecuteStatementRequest request) {
        return client.executeStatement(request);
    }
    
    
    public UrlData marshallIntoUrlData(ExecuteStatementResult result) {
        return mapper.marshallIntoObjects(UrlData.class, result.getItems()).get(0);
    }
    

    public DocumentData getDocumentData(String md5) {
        DocumentData item = mapper.load(DocumentData.class, md5);
        logger.info("Retrieved from " + DOCUMENT_TABLE);
        return item;
    }

    public List<DocumentData> batchGetDocumentData(Iterable<String> md5s) {

        TableKeysAndAttributes hashKeys = new TableKeysAndAttributes(DOCUMENT_TABLE);
        for (String md5 : md5s) {
            hashKeys = hashKeys.addHashOnlyPrimaryKey("md5", md5);
        }
        BatchGetItemSpec spec = new BatchGetItemSpec().withTableKeyAndAttributes(hashKeys);

        List<DocumentData> results = new ArrayList<>();
        db.batchGetItem(spec).getTableItems().get(DOCUMENT_TABLE).forEach(item -> {
            try {
                results.add(om.readValue(item.toJSON(), DocumentData.class));
            } catch (JsonProcessingException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        });
        return results;

    }

    public InvertedIndexData getInvertedIndexData(String word, String md5) {
        InvertedIndexData item = mapper.load(InvertedIndexData.class, word, md5);
        logger.info("Retrieved from " + INVERTED_INDEX_TABLE);
        return item;
    }

    public List<InvertedIndexData> batchGetInvertedIndexData(Iterable<String> words, Iterable<String> md5s) {
        Iterator<String> wordsIter = words.iterator();
        Iterator<String> md5sIter = md5s.iterator();

        TableKeysAndAttributes hashKeys = new TableKeysAndAttributes(INVERTED_INDEX_TABLE);
        for (String word = wordsIter.next(), md5 = md5sIter.next(); wordsIter.hasNext()
                && md5sIter.hasNext(); word = wordsIter.next(), md5 = md5sIter.next()) {
            hashKeys = hashKeys.addHashAndRangePrimaryKey("word", word, "md5", md5);
        }

        List<InvertedIndexData> results = new ArrayList<>();
        db.batchGetItem(hashKeys).getTableItems().get(INVERTED_INDEX_TABLE).forEach(item -> {
            try {
                results.add(om.readValue(item.toJSON(), InvertedIndexData.class));
            } catch (JsonProcessingException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        });
        return results;

    }

    public List<InvertedIndexData> getInvertedIndexData(String word) {
        QuerySpec querySpec = new QuerySpec()
                .withKeyConditionExpression("word = :word")
                .withValueMap(new ValueMap().withString(":word", word));

        List<InvertedIndexData> results = new ArrayList<>();
        db.getTable(INVERTED_INDEX_TABLE).query(querySpec).forEach(item -> {
            try {
                results.add(om.readValue(item.toJSON(), InvertedIndexData.class));
            } catch (JsonProcessingException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        });

        return results;
    }

    public List<InvertedIndexData> batchGetInvertedIndexData(Iterable<String> words) {
        List<InvertedIndexData> results = new ArrayList<>();
        // PartiQL
        ExecuteStatementRequest request = new ExecuteStatementRequest().withStatement(
                String.format("SELECT * FROM %s WHERE word IN ['%s']", INVERTED_INDEX_TABLE,
                        String.join("','", ImmutableList.copyOf(words))));

        client.executeStatement(request).getItems().forEach(item -> {
            results.add(new InvertedIndexData() {{
                setWord(item.get("word").getS());
                setMd5(item.get("md5").getS());
                setCount(Integer.parseInt(item.getOrDefault("count", new AttributeValue().withN("0")).getN()));
                setTf(Float.parseFloat(item.getOrDefault("tf", new AttributeValue().withN("0")).getN()));
                setIdf(Float.parseFloat(item.getOrDefault("idf", new AttributeValue().withN("0")).getN()));
                setTfidf(Float.parseFloat(item.getOrDefault("tfidf", new AttributeValue().withN("0")).getN()));
            }});
        });

        return results;
    }

    // U --------------------------------------------------
    private <T> void updateData(T item) {
        DynamoDBMapperConfig dynamoDBMapperConfig = new DynamoDBMapperConfig.Builder()
                .withConsistentReads(DynamoDBMapperConfig.ConsistentReads.CONSISTENT)
                .withSaveBehavior(DynamoDBMapperConfig.SaveBehavior.UPDATE)
                .build();
        mapper.save(item, dynamoDBMapperConfig);
    }

    private <T> void batchUpdateData(Iterable<T> items) {
        mapper.batchSave(items);
    }

    public void updateUrlData(UrlData item) {
        updateData(item);
        logger.info("Updated " + URL_TABLE);
    }

    public void batchUpdateUrlData(Iterable<UrlData> items) {
        batchUpdateData(items);
        logger.info("Updated " + URL_TABLE);
    }

    public void updateDocumentData(DocumentData item) {
        updateData(item);
        logger.info("Updated " + DOCUMENT_TABLE);
    }

    public void batchUpdateDocumentData(Iterable<DocumentData> items) {
        batchUpdateData(items);
        logger.info("Updated " + DOCUMENT_TABLE);
    }

    public void updateInvertedIndexData(InvertedIndexData item) {
        updateData(item);
        logger.info("Updated " + INVERTED_INDEX_TABLE);
    }

    public void batchUpdateInvertedIndexData(Iterable<InvertedIndexData> items) {
        batchUpdateData(items);
        logger.info("Updated " + INVERTED_INDEX_TABLE);
    }

    // D --------------------------------------------------
    public void deleteUrlData(String url) {
        UrlData item = new UrlData();
        item.setUrl(url);
        mapper.delete(item);
        logger.info("Deleted " + URL_TABLE);
    }

    public void deleteDocumentData(String md5) {
        DocumentData item = new DocumentData();
        item.setMd5(md5);
        mapper.delete(item);
        logger.info("Deleted " + DOCUMENT_TABLE);
    }

    public void deleteInvertedIndexData(String word, String md5) {
        InvertedIndexData item = new InvertedIndexData();
        item.setWord(word);
        item.setMd5(md5);
        mapper.delete(item);
        logger.info("Deleted " + INVERTED_INDEX_TABLE);
    }

    // NON STABLE APIS ------------------------------------
    // for crawler ----------------------------------------
    public String getMd5(String url) {
        UrlData item = getUrlData(url);
        return item != null ? item.getMd5() : "null";
    }
    
    public List<String> getUrls(String md5) {
        QuerySpec querySpec = new QuerySpec()
                .withKeyConditionExpression("md5 = :md5")
                .withValueMap(new ValueMap().withString(":md5", md5))
                .withProjectionExpression("#url")
                .withNameMap(new NameMap().with("#url", "url"))
                .withMaxResultSize(10);

        List<String> results = new ArrayList<>();
        db.getTable(URL_TABLE).getIndex("md5-weight-index").query(querySpec).forEach(item -> {
            results.add(item.getString("url"));
        });

        return results;
    }
	
    public static void main(String[] args) {
        org.apache.logging.log4j.core.config.Configurator.setLevel("edu.upenn.cis", Level.DEBUG);
        DynamoDBInterface db = new DynamoDBInterface();
        
        // table that will be used for scanning
//        List<String> md5 = Arrays.asList("ffb54deafc01f59fee2828b402a137a1", "ff4f719e77203454f785725cbc563498");
//        
//        // number of items each scan request should return
//        int scanItemLimit = 1000;
//        
//        // number of logical segments for parallel scan
//        int parallelScanThreads = md5.size();
//        
//        try {
//			List<String> res = db.parallelScan(scanItemLimit, parallelScanThreads, md5);
//			
//		} catch (InterruptedException | ExecutionException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
        
        // ##################################  FOR INDEXER #################################
        // ################## DOWNLOAD DOCUMENTS FROM DOCUMENT DB ##########################
//        ScanSpec scanSpec = new ScanSpec().withAttributesToGet("md5", "document");
//
//        db.db.getTable("DOCUMENT").scan(scanSpec).pages().forEach(page -> {
//        page.forEach(item -> {
//          try {
//        	  String filename= "MyFile.txt";
//              FileWriter fw = new FileWriter(filename,true); //the true will append the new data
//              fw.write(item.getString("md5") + "\t");//appends the string to the file
//              String formattedDoc = item.getString("document");
//              String rawContent = DynamoDBInterface.getHTMLText(formattedDoc);
//              
//              fw.write(rawContent + "\t");
//              fw.write("\n");
//              fw.close();
////           System.out.println(item.get("md5"));
//          } catch (IOException e) {
//        	  e.printStackTrace();
////              exception handling left as an exercise for the reader
//          }
//         }); 
//        });
        
        // ################## UPLOAD WORDS, MD5 AND SCORE TO INVIDX ##################
//        Path file = new Path("reducerOutput.txt");
//        System.out.println("Starting the upload");
//
//        Configuration conf = new Configuration();
//        List<InvertedIndexData> res = new ArrayList<>();
//        
//        FileSystem fs;
//        LineIterator iter;
//		try {
//			fs = file.getFileSystem(conf);
//			iter = IOUtils.lineIterator(fs.open(file), "UTF8");
//			int i = 0;
//	        while (iter.hasNext()) {
//		         String line = iter.nextLine();
//		         String[] parts = line.split("\t");
////		         System.out.println(parts.length);
//
//		         System.out.println(parts[0] + " " + parts[1] + " " + parts[2] + " " + parts[3]);
//		         
//	        	 InvertedIndexData item = new InvertedIndexData();
//			     item.setWord(parts[0]);
//			     item.setMd5(parts[1]);
//			     item.setIdf(Float.parseFloat(parts[2]));
//			     item.setTfidf(Float.parseFloat(parts[3]));
////			     db.updateInvertedIndexData(item);
//			     res.add(item); 
//			     i++;
//			     if (i % 10000 == 0) {
//			    	 System.out.println("============Uploading to DB============");
//			    	 db.batchUpdateInvertedIndexData(res);
//			    	 i = 0; 
//			    	 res = new ArrayList<>();
//			     }
//	       }
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		db.batchUpdateInvertedIndexData(res);
	    // ##############################################################################

        // db.updateInvertedIndexData(new InvertedIndexData() {{
        // setWord("word-2");
        // setMd5("md5-2");
        // setCount(1);
        // setTf(2.1f);
        // }});

        // db.getInvertedIndexData("word").forEach(item -> {
        // try {
        // System.out.println(db.om.writerWithDefaultPrettyPrinter().writeValueAsString(item));
        // } catch (JsonProcessingException e) {
        // // TODO Auto-generated catch block
        // e.printStackTrace();
        // }
        // });

        // db.batchGetUrlData(new ArrayList<String>() {{
        // add("http://www.aol.com/news/xxx");
        // }}).forEach(item -> {
        // System.out.println(DateUtils.getDateString(item.getDate()));
        // try {
        // System.out.println(db.om.writerWithDefaultPrettyPrinter().writeValueAsString(item));
        // } catch (JsonProcessingException e) {
        // // TODO Auto-generated catch block
        // e.printStackTrace();
        // }
        // });
        // System.out.println(Integer.parseInt(new AttributeValue("0").getN()));
        // db.batchGetInvertedIndexData(Arrays.asList("word", "word-2")).forEach(item -> {
            
        //     // try {
        //     //     System.out.println(db.om.writerWithDefaultPrettyPrinter().writeValueAsString(item));
        //     // } catch (JsonProcessingException e) {
        //     //     // TODO Auto-generated catch block
        //     //     e.printStackTrace();
        //     // }
        // });
        // ScanSpec scanSpec = new ScanSpec().withAttributesToGet("url");

        // db.db.getTable("URL").scan(scanSpec).pages().forEach(page -> {
        // page.forEach(item -> {
        // System.out.println(item.getString("url"));
        // });
        // });

        // Test CRUD Operations
        // Test ADD urlMd5Db
        // db.addMd5("url-1-1", "md5-1");
        // db.addMd5("url-1-2", "md5-1");
        // db.addMd5("url-1-3", "md5-1");
        // db.addMd5("url-2-1", "md5-2");
        // db.addMd5("url-2-2", "md5-2");
        // db.addMd5("url-3-1", "md5-3");
        // db.deleteUrlMd5Data("url-3-1");

        // update can also create data
        // db.updateUrlMd5Data(new UrlMd5Data() {{
        // setUrl("url-1-1");
        // setMd5("md5-3");
        // }});

        // db.updateDocumentData(new DocumentData() {{
        // setMd5("haha");
        // setDocument("??");
        // }});

        // db.getMd5("abc");

        // System.out.println(
        // db.getUrls(db.getMd5("url-1-1"))
        // );
        // db.addMd5("http://poogle.com", "test");
        // Test GET urlMd5Db
        // System.out.println(db.getMd5("http://google.com"));
        // Test UPDATE urlMd5Db
        // db.updateMd5("http://poogle.com", "testtest");

        // Test ADD md5DocDb
        // db.addDocument("http://poogle.com", "test", "test", new
        // HashSet<String>(Arrays.asList("a", "b")), new
        // HashSet<String>(Arrays.asList("c", "d")));
        // TEST GET md5DocDb
        // System.out.println(db.getDoc("http://poogle.com"));
        // TEST UPDATE md5DocDb
        // db.updateDoc("http://poogle.com", "test1", "test1", new
        // HashSet<String>(Arrays.asList("a", "b")), new
        // HashSet<String>(Arrays.asList("c", "d")));
        
        // ################################## For Page Rank #############################
//      // for outputing file from database
//      
//      ScanSpec scanSpec = new ScanSpec().withAttributesToGet("url", "outboundLinks");
//      
//      db.db.getTable("URL").scan(scanSpec).pages().forEach(page -> {
//      	page.forEach(item -> {
//      		try {
//      			String filename= "MyFile.txt";
//      		    FileWriter fw = new FileWriter(filename,true); //the true will append the new data
//      		    fw.write(item.getString("url") + "\t");//appends the string to the file
//      		    for (String url: (LinkedHashSet<String>)item.get("outboundLinks")) {
//      		    	fw.write(url + "\t");
//      		    }
//      		    fw.write("\n");
//      		    fw.close();
////      			System.out.println(item.get("outboundLinks"));
//      		}catch (IOException e) {
////      		    exception handling left as an exercise for the reader
//      		}
//      	});	
//      });
      
      // for updating pagerank weights to database
      
//      Path file = new Path("output/output.txt");
//      
//      Configuration conf = new Configuration();
//	    
//	    FileSystem fs = file.getFileSystem(conf);
//      LineIterator iter = IOUtils.lineIterator(fs.open(file), "UTF8");
//      
//      while (iter.hasNext()) {
//	      String line = iter.nextLine();
// 
//	      String[] parts = StringUtils.split(line);
//	      
//	      try {
//	    	UrlData item = db.getUrlMd5Data(parts[0]);
//			item.setWeight(Float.parseFloat(parts[1]));
//			db.updateUrlMd5Data(item);
//	      } catch (Exception e) {
//			e.printStackTrace();
//	      }
//	    }

    }
}
