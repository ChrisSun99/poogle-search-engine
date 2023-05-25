# Poogle Search Engine

The repository of Poogle search engine. Final report is available [here](555_Search_Engine_report.pdf).

![](demo.gif)

## Getting Started
We use [Apache Maven](https://maven.apache.org/) to manage compilation, testing, and execution. 
### Prerequisites ###
We are using [AWS DynamoDB](https://aws.amazon.com/dynamodb/?trk=ea446940-00bb-4bee-9f27-d7a9a8080e4d&sc_channel=ps&sc_campaign=acquisition&sc_medium=ACQ-P|PS-GO|Brand|Desktop|SU|Database|DynamoDB|US|EN|Text&s_kwcid=AL!4422!3!488982705294!e!!g!!dynamodb&ef_id=CjwKCAjwgr6TBhAGEiwA3aVuITbNVHHUDnxx7XmUmzdLj_FF558vAq6GuZxoLjFzyk0VR6gzDO9TfhoCHwsQAvD_BwE:G:s&s_kwcid=AL!4422!3!488982705294!e!!g!!dynamodb).
To run the code, you'll need to create the following DynamoDB tables.  <br>
* `URL`: uses `url` as the partition key and contains a global secondary index `md5-weight-index` which uses `md5` as the partition key and `weight` as the sort key. The table also has attributes `date` and `outboundLinks`. </br>
* `DOCUMENT`: uses `md5` as the partition key and contains attributes `date` and `document`.<br>
* `INVIDX`: uses `word` as the partition key and `md5` as the sort key.<br>


### Installation ###
If trying to run each part of the code, you'll need to add and apply the following command in `Run Configuration`

```
clean install exec:java@crawler   # Run the crawler and update the database

clean install exec:java@pagerank  # Run the pagerank MapReduce job and update the database

clean install exec:java@indexer   # Run the indexer MapReduce job and update the database

clean install exec:java@server    # Start the search engine server
```

For the frontend on the client side, you'll need to go to ./client directory and execute `npm start`. Open http://localhost:3000 to view the web page in a browser. 


## Features ###
### Crawler 

We have two major version for crawler, one implemented with ThreadPool, and another implemented with Apache Storm and Kafka.

Main entrypoint is `edu.upenn.cis.cis455.crawler.Crawler`, currently all crawler file is on `crawler-k8s-threadpool` branch. The crawler uses bloom filter to remove duplicate and seen urls, stores a LRU cache for robots.txt, and group several urls for batch updates. Non-HTML content is parsed with Apache Tika. All web metadata and documents are stored in Amazon DynamoDB.

We intended to deploy the thread-pool version of distributed crawler with kubernetes, each crawler functions as a seperate program except they will share a common url queue, hosted by Amazon SQS. We will also explore the more powerful distributed crawler implemented with Apache Storm and Kafka. Our plan is to host our Storm and Kafka (nimbus, zookeepers, supervisors, etc.) on a kubernetes cluster.

### PageRank 
We have implemented an EMR-based PageRank. 

Main function is located at `edu.upenn.cis.cis455.pagerank.PageRankInterface`, it takes in three arguments:

1. Input file location containing urls and their outbound links.
2. Desired output directory.
3. A boolean that is true if we want to distribute less weight to intra-domain links, false if we want to treat intra-domain and inter-domain links the same.

### Indexer 
We have implemented an EMR-based indexer that creates inverted index for the crawled document corpus. 

### Search Engine 
We used [React.js](https://reactjs.org/) to develop the frontend of the search engine. We developed the frontend with reference to this [Medium article](https://betterprogramming.pub/building-a-google-clone-part-1-setting-up-react-fb9c22b9662c). Codes were adopted from https://github.com/5ebs/Google-Clone with large modification. Users are able to see the url link and a snippet of preview of the 
web page. Our search engine caches the search results to BerkeleyDB. When a user searches the same query, the search engine will respond quickly with the result from cache. 

## Extra Credits ###

1. Crawler can handle non-html data.
2. Crawler can store partial metadata about the web documents.
3. Indexer uses metadata of web pages to improve the rankings

## Source Files 
* Crawler: `edu.upenn.cis.cis455.crawler`
* PageRank: `edu.upenn.cis.cis455.pagerank`
* Indexer: `edu.upenn.cis.cis455.indexer`
* Search Engine: `edu.upenn.cis.cis455.searchengine`


