package edu.upenn.cis.cis455.crawler.distributed.bolt;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.jsoup.Connection.Response;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import edu.upenn.cis.cis455.crawler.utils.DateUtils;
import edu.upenn.cis.cis455.crawler.utils.TikaUtils;
import edu.upenn.cis.cis455.crawler.utils.UrlUtils;

public class DocParseBolt extends BaseBasicBolt {

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String url = input.getStringByField("url");
        Response response = (Response) input.getValueByField("response");
        try{
            // prepareing document
            Date date = DateUtils.parseDateString(response.header("Date"));
            String document = TikaUtils.parseContent(response.bodyStream(), -1);
            if (!TikaUtils.detectLanguage(document).equals("en")) {
                return;
            }
            // find all links in the document if html
            Set<String> outboundLinks = new HashSet<>();
            if (TikaUtils.isHTML(response.contentType())) {
                Document doc = response.parse();
                for (Element linkElem : doc.select("a[href]")) {
                    String link = UrlUtils.normalize(linkElem.attr("abs:href"));
                    outboundLinks.add(link);
                    collector.emit(new Values(link));
                }
            }
            collector.emit("DB", new Values(url, date, document, outboundLinks));
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // default stream
        declarer.declare(new Fields("url"));
        // DB operation stream
        declarer.declareStream("DB", new Fields("url", "date", "document", "outboundLinks"));
    }
    
}
