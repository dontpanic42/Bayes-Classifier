import java.io.Serializable;
import java.util.Map;
import java.util.HashMap;

/**
 * Created by daniel on 13.06.15.
 */
public class Dictionary implements Serializable {

    int maxIndex = 0;
    int numDocs = 0;
    Map<Integer, String> idxToTerm = new HashMap<>();
    Map<String, Integer> termToIdx = new HashMap<>();
    Map<String, Integer> termToDocCount = new HashMap<>();

    synchronized public void addDocument(Document doc) {
        numDocs++;

        for(String term : doc.getTermsSet()) {
            if(termToIdx.containsKey(term)) {
                termToDocCount.put(term, termToDocCount.get(term) + 1);
            } else {
                termToDocCount.put(term, 1);
                int idx = maxIndex++;
                idxToTerm.put(idx, term);
                termToIdx.put(term, idx);
            }
        }
    }

    public int size() {
        return idxToTerm.size();
    }

    public int getIndex(String term) {
        return termToIdx.get(term);
    }

    public String getTerm(int index) {
        return idxToTerm.get(index);
    }

    public boolean contains(String term) {
        return termToIdx.containsKey(term);
    }

    public double getIDFS(String term) {
       return ((double) numDocs) / (double) termToDocCount.get(term);
    }
}
