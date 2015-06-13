import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by daniel on 13.06.15.
 */
public class Tokenizer implements Serializable {

    private static final int maxData = 1600000;

    public List<Document> tokenize(JavaSparkContext sc, File file) throws IOException {
        if(!file.exists()) {
            throw new FileNotFoundException();
        }

        List<Document> docs = new ArrayList<>();
        CSVParser parser = CSVParser.parse(file, StandardCharsets.UTF_8, CSVFormat.DEFAULT);
        int i = 0;
        for(CSVRecord rec : parser) {
            Document doc = new Document();
            doc.setLabel(Double.valueOf(rec.get(0)));
            doc.setBody(rec.get(5));
            docs.add(doc);
            if(i++ > maxData) {
                break;
            }
        }

        parser.close();

        return sc.parallelize(docs).map(doc -> tokenizeDocument(doc)).collect();
    }

    private Document tokenizeDocument(Document doc) throws IOException {
        StringReader reader = new StringReader(doc.getBody());
        EnglishAnalyzer anal = new EnglishAnalyzer();
        TokenStream stream = anal.tokenStream("contents", reader);
        CharTermAttribute term = stream.addAttribute(CharTermAttribute.class);
        stream.reset();

        while(stream.incrementToken()) {
            String str = term.toString();
            doc.getTermsSet().add(str);
            doc.getTermsList().add(str);
        }

        return doc;
    }
}
