package edu.upenn.cis.cis455.crawler.utils;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Arrays;

import org.apache.tika.Tika;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.sax.BodyContentHandler;
import org.jsoup.Jsoup;
import org.jsoup.Connection.Method;
import org.jsoup.Connection.Response;
import org.apache.tika.language.detect.LanguageDetector;
import org.apache.tika.langdetect.optimaize.OptimaizeLangDetector;
import org.apache.tika.metadata.Metadata;

public class TikaUtils {

    static final AutoDetectParser parser = new AutoDetectParser();
    static final LanguageDetector detector = new OptimaizeLangDetector().loadModels();

    public static boolean isHTML(String mimeType) {
        if (mimeType == null) return false;
        String normalizedMimeType = mimeType.toLowerCase();
        return normalizedMimeType.contains("html");
    }

    public static boolean isXML(String mimeType) {
        if (mimeType == null) return false;
        String normalizedMimeType = mimeType.toLowerCase();
        return normalizedMimeType.contains("xml") || normalizedMimeType.contains("rss");
    }

    public static boolean isTEXT(String mimeType) {
        if (mimeType == null) return false;
        String normalizedMimeType = mimeType.toLowerCase();
        return normalizedMimeType.contains("text");
    }

    public static boolean isHTMLorXML(String mimeType) {
        return isHTML(mimeType) || isXML(mimeType);
    }
    
    public static boolean isHTMLorXMLorTEXT(String mimeType) {
        return isHTML(mimeType) || isXML(mimeType) || isTEXT(mimeType);
    }

    public static String parseContent(InputStream in, int writeLimit) throws Exception {
        BodyContentHandler handler = new BodyContentHandler(writeLimit);
        Metadata metadata = new Metadata();
        
        parser.parse(in, handler, metadata);

        return handler.toString().replace("\\s+", " ");
    }

    public static String detectLanguage(String content) {
        return detector.detect(content).getLanguage();
    }

    public static void main(String[] args) throws Exception {
        // AutoDetectParser parser = new AutoDetectParser();
        // BodyContentHandler handler = new BodyContentHandler(1024 * 1024);
        // Tika tika = new Tika();
        // // // // tika.parse(url, metadata)
        // Metadata metadata = new Metadata();
        // // // // InputStream is = Jsoup.connect("https://arxiv.org/pdf/2204.10326").execute().bodyStream();
        // InputStream in = (new URL("https://arxiv.org/pdf/2204.10326")).openStream();
        // // // Document doc = Jsoup.parse(in, null, "https://arxiv.org/pdf/2204.10326");
        // Response res = Jsoup.connect("https://arxiv.org/pdf/2204.10326").method(Method.HEAD).ignoreContentType(true).maxBodySize(1024).execute();
        // System.out.println(res.contentType());
        // System.out.println(res.headers());

        // try (
        //     Reader jsoupReader = new BufferedReader(new InputStreamReader(res.bodyStream(), Charset.forName(StandardCharsets.UTF_8.name())));
        //     Reader urlReader = new BufferedReader(new InputStreamReader(in, Charset.forName(StandardCharsets.UTF_8.name())));
        // ) {
        //     int a = 0;
        //     int b = 0;
        //     while (true) {
        //         int aReadResult = (a = jsoupReader.read());
        //         int bReadResult = (b = urlReader.read());
        //         if (aReadResult == -1 || bReadResult == -1) {
        //             System.out.println(aReadResult + " " + bReadResult);
        //             break;
        //         }
        //         if (a != b) {
        //             System.out.println("!");
        //             break;
        //         }
        //     }
        //     while ((b = urlReader.read()) != -1) {
        //         System.out.print(b);
        //     }
        // }

        // System.out.println(res.toString());
        // parser.parse(res.bodyStream(), handler, metadata);
        // // parser.parse(, handler, metadata);
        // System.out.println(
        //     // tika.parseToString(res.bodyStream(), metadata)
        //     handler.toString()
        // );
        // for (String name : metadata.names()) {
        //     System.out.println(name + ": " + metadata.get(name));
        // }
        // System.out.println(detectLanguage(handler.toString()));

    }
    
}
