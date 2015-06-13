import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.rdd.RDD;

import java.io.*;
import java.util.Date;
import java.util.List;

/**
 * Created by daniel on 13.06.15.
 */
public class Main {

    public static void main(String[] args) {
        //doTraining();

        doPrediction();
    }

    private static void doPrediction() {
        System.out.println("Prediction");
        JavaSparkContext sc = new JavaSparkContext("local[4]", "naivebayes");
        Dictionary dict = readDict();
        NaiveBayesModel model = readModel(sc);
    }

    private static Dictionary readDict() {
        try {
            InputStream file = new FileInputStream("/Users/daniel/Desktop/tcDict.mo");
            InputStream buffer = new BufferedInputStream(file);
            ObjectInput input = new ObjectInputStream(buffer);
            Dictionary dict = (Dictionary) input.readObject();
            input.close();
            buffer.close();
            file.close();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }

        return null;
    }

    private static NaiveBayesModel readModel(JavaSparkContext sc) {
        NaiveBayesModel model = NaiveBayesModel.load(sc.sc(), "/Users/daniel/Desktop/tcModel");
        return model;
    }

    private static void saveDict(Dictionary dict) {
        try {
            OutputStream file = new FileOutputStream("/Users/daniel/Desktop/tcDict.mo");
            OutputStream buffer = new BufferedOutputStream(file);
            ObjectOutput output = new ObjectOutputStream(buffer);
            output.writeObject(dict);
            output.close();
            buffer.close();
            file.close();
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    private static void saveModel(JavaSparkContext sc, NaiveBayesModel model) {
        model.save(sc.sc(), "/Users/daniel/Desktop/tcModel");
    }

    private static void doTraining() {

        File trainingFile = new File("/Users/daniel/Downloads/trainingandtestdata/training.1600000.processed.noemoticon.csv");
        //File trainingFile = new File("/Users/daniel/Downloads/trainingandtestdata/testdata.manual.2009.06.14.csv");
        JavaSparkContext sc = new JavaSparkContext("local[4]", "naivebayes");

        Dictionary dict = new Dictionary();
        NaiveBayesModel model = train(sc, dict, trainingFile);
        //model.save(sc.sc(), "/Users/daniel/Deskotp/model1600000");

        saveDict(dict);
        saveModel(sc, model);

        //sc.close();
    }

    private static NaiveBayesModel train(JavaSparkContext sc, Dictionary dict, File trainingFile) {
        List<Document> docs = null;
        Tokenizer tokenizer = new Tokenizer();

        try {
            System.out.println(new Date().toString() + " Tokenizing.");
            docs = tokenizer.tokenize(sc, trainingFile);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        System.out.println("Num docs: " + docs.size());

        System.out.println(new Date().toString() + " Splitting.");
        JavaRDD[] data = sc.parallelize(docs).randomSplit(new double[]{0.9, 0.1});
        JavaRDD<Document> trDocs = data[0];
        JavaRDD<Document> teDocs = data[1];


        System.out.println(new Date().toString() + " Creating dict.");
        System.out.println("Traing doc size: " + trDocs.collect().size());

        for(Document doc : trDocs.collect()) {
            dict.addDocument(doc);
        }

        System.out.println("Dict size: " + dict.size());


        System.out.println(new Date().toString() + " Vectorizing.");
        RDD<LabeledPoint> rdd = JavaRDD.toRDD(trDocs.map(doc -> doc.getPoint(dict)));

        System.out.println(new Date().toString() + " Training.");

        NaiveBayesModel model = NaiveBayes.train(rdd);

        System.out.println(new Date().toString() + " Testing.");
        int correctPredictions = teDocs
                .filter(doc -> model.predict(doc.vectorize(dict)) == doc.getLabel())
                .collect()
                .size();
        int testSize = teDocs.collect().size();

        System.out.println("correct: " + correctPredictions + " num: " + testSize);
        double acc = ((double) correctPredictions) / ((double) testSize);
        System.out.println("accuracy: " + acc);

        return model;
    }
}
