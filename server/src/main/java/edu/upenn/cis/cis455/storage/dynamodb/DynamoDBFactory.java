package edu.upenn.cis.cis455.storage.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;

/** 
 *  Initiate database connection 
 *
 */
public class DynamoDBFactory {
    private final static String AWS_PROFILE_NAME = "default";
    
    private final static AmazonDynamoDBClientBuilder builder =
            AmazonDynamoDBClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .withCredentials(new ProfileCredentialsProvider(AWS_PROFILE_NAME));

    public static AmazonDynamoDB createClient() {
        return builder.build();
    }
}
