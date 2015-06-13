import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import scala.Tuple2;
import org.apache.spark.mllib.linalg.Vector;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by daniel on 13.06.15.
 */
public class Document implements Serializable {

    private String body;
    private double label;

    private List<String> termsList = new ArrayList<>();
    private Set<String> termsSet = new HashSet<>();

    public List<String> getTermsList() {
        return termsList;
    }

    public Set<String> getTermsSet() {
        return termsSet;
    }

    public double getTF(String term) {
        return (double) Collections.frequency(termsList, term);
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }

    public double getLabel() {
        return label;
    }

    public void setLabel(double label) {
        this.label = label;
    }

    public Vector vectorize(Dictionary dict) {
        return Vectors.sparse(
                dict.size(),
                termsSet.stream()
                        .filter(term -> dict.contains(term))
                        .map(term -> new Tuple2<Integer, Double>(
                                dict.getIndex(term),
                                dict.getIDFS(term) * getTF(term)))
                        .collect(Collectors.toList())
        );
    }

    public LabeledPoint getPoint(Dictionary dict) {
        return new LabeledPoint(label, vectorize(dict));
    }
}
