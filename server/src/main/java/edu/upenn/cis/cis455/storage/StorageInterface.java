package edu.upenn.cis.cis455.storage;

import java.util.Date;
import java.util.Set;

import edu.upenn.cis.cis455.storage.dynamodb.DynamoDBInterface;
import edu.upenn.cis.cis455.storage.dynamodb.DocumentData;
import edu.upenn.cis.cis455.storage.dynamodb.UrlData;
import edu.upenn.cis.cis455.storage.utils.StringUtils;

public class StorageInterface {
    
    private DynamoDBInterface dynamoDb;

    public StorageInterface() {
        dynamoDb = new DynamoDBInterface();
    }

    public boolean addDocument(String url, Date date, String document, Set<String> outboundLinks) {

        // String md5 = StringUtils.hash(document, "MD5");
        // String oldMd5 = dynamoDb.getMd5(url);

        // dynamoDb.updateUrlMd5Data(new UrlData() {{
        //     setUrl(url);
        //     setMd5(md5);
        //     setDate(date);
        //     setOutboundLinks(outboundLinks);
        // }});
        
        // // remove redundant oldMd5-doc mapping if no url-oldMd5 mapping exists
        // if (dynamoDb.getUrls(oldMd5).isEmpty()) {
        //     dynamoDb.deleteMd5DocData(oldMd5);
        // }

        // dynamoDb.updateMd5DocData(new DocumentData() {{
        //     setMd5(md5);
        //     setDocument(document);
        //     setDate(date);
        // }});

        // return !md5.equals(oldMd5);
        return false;

    }

    public void close() {
        // dynamoDb.close();
    }

}
