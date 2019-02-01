/*
Single Author Info:
nphabia Niklesh Ashok Phabiani
*/
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.*;

/*
 * Main class of the TFIDF Spark implementation.
 * Author: Tyler Stocksdale
 * Date:   10/31/2017
 */
public class TFIDF {

	static boolean DEBUG = true;

    public static void main(String[] args) throws Exception {
        // Check for correct usage
        if (args.length != 1) {
            System.err.println("Usage: TFIDF <input dir>");
            System.exit(1);
        }

		// Create a Java Spark Context
		SparkConf conf = new SparkConf().setAppName("TFIDF");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Load our input data
		// Output is: ( filePath , fileContents ) for each file in inputPath
		String inputPath = args[0];
		JavaPairRDD<String,String> filesRDD = sc.wholeTextFiles(inputPath);

		// Get/set the number of documents (to be used in the IDF job)
		long numDocs = filesRDD.count();

		//Print filesRDD contents
		if (DEBUG) {
			List<Tuple2<String, String>> list = filesRDD.collect();
			System.out.println("------Contents of filesRDD------");
			for (Tuple2<String, String> tuple : list) {
				System.out.println("(" + tuple._1 + ") , (" + tuple._2.trim() + ")");
			}
			System.out.println("--------------------------------");
		}

		/*
		 * Initial Job
		 * Creates initial JavaPairRDD from filesRDD
		 * Contains each word@document from the corpus and also attaches the document size for
		 * later use
		 *
		 * Input:  ( filePath , fileContents )
		 * Map:    ( (word@document) , docSize )
		 */
		JavaPairRDD<String,Integer> wordsRDD = filesRDD.flatMapToPair(
			new PairFlatMapFunction<Tuple2<String,String>,String,Integer>() {
				public Iterable<Tuple2<String,Integer>> call(Tuple2<String,String> x) {
					// Collect data attributes
					String[] filePath = x._1.split("/");
					String document = filePath[filePath.length-1];
					String fileContents = x._2;
					String[] words = fileContents.split("\\s+");
					int docSize = words.length;

					// Output to Arraylist
					ArrayList ret = new ArrayList();
					for(String word : words) {
						ret.add(new Tuple2(word.trim() + "@" + document, docSize));
					}
					return ret;
				}
			}
		);

		//Print wordsRDD contents
		if (DEBUG) {
			List<Tuple2<String, Integer>> list = wordsRDD.collect();
			System.out.println("------Contents of wordsRDD------");
			for (Tuple2<String, Integer> tuple : list) {
				System.out.println("(" + tuple._1 + ") , (" + tuple._2 + ")");
			}
			System.out.println("--------------------------------");
		}

		/*
		 * TF Job (Word Count Job + Document Size Job)
		 * Gathers all data needed for TF calculation from wordsRDD
		 *
		 * Input:  ( (word@document) , docSize )
		 * Map:    ( (word@document) , (1/docSize) )
		 * Reduce: ( (word@document) , (wordCount/docSize) )
		 */
		JavaPairRDD<String,String> tfRDD = wordsRDD.mapToPair(
			new PairFunction<Tuple2<String,Integer>,String,String>() {
				public Tuple2<String, String> call(Tuple2<String, Integer> wordAtDocumentWithDocSize) {
					// Retrieving (word@document) and (docSize)
					String wordAtDocument = wordAtDocumentWithDocSize._1;
					int docSize = wordAtDocumentWithDocSize._2;
					String value = 1 + "/" + docSize;
					return (new Tuple2(wordAtDocument, value));
				}
			}
		).reduceByKey(
			new Function2<String, String, String>() {
				public String call(String v1, String v2) {
					// Splitting intermediate counts (1/docSize) or (someIntermediateValue/docSize)
					String[] countSlashDocSizeV1 = v1.split("/");
					String[] countSlashDocSizeV2 = v2.split("/");
					String docSize = countSlashDocSizeV1[1];
					// Storing the two word counts coming in to reduce function as int
					int countV1 = Integer.parseInt(countSlashDocSizeV1[0]);
					int countV2 = Integer.parseInt(countSlashDocSizeV2[0]);
					int currentWordCount = countV1 + countV2;
					// Constructing the new value i.e (someIntermediateValue/docSize)
					// or final (wordCount/docSize) if this is the last computation for
					// for a particular key
					String wordCountSlashDocSize = currentWordCount + "/" + docSize;
					return wordCountSlashDocSize;
				}
			}
		);

		//Print tfRDD contents
		if (DEBUG) {
			List<Tuple2<String, String>> list = tfRDD.collect();
			System.out.println("-------Contents of tfRDD--------");
			for (Tuple2<String, String> tuple : list) {
				System.out.println("(" + tuple._1 + ") , (" + tuple._2 + ")");
			}
			System.out.println("--------------------------------");
		}

		/*
		 * IDF Job
		 * Gathers all data needed for IDF calculation from tfRDD
		 *
		 * Input:  ( (word@document) , (wordCount/docSize) )
		 * Map:    ( word , (1/document) )
		 * Reduce: ( word , (numDocsWithWord/document1,document2...) )
		 * Map:    ( (word@document) , (numDocs/numDocsWithWord) )
		 */
		JavaPairRDD<String,String> idfRDD = tfRDD.mapToPair(
			/************ YOUR CODE HERE ************/
			new PairFunction<Tuple2<String,String>,String,String>() {
				public Tuple2<String, String> call(Tuple2<String, String> wordAtDocumentWithTermFrequency) {
					// Retrieving (word@document) and (term frequency i.e wordCount/dcSize)
					String wordAtDocument = wordAtDocumentWithTermFrequency._1;
					String termFrequency = wordAtDocumentWithTermFrequency._2;
					String[] wordAtDocumentSplitted = wordAtDocument.split("@");
					String word = wordAtDocumentSplitted[0];
					String document = wordAtDocumentSplitted[1];
					// Emitting (word) and (1/document)
					return (new Tuple2(word, 1 + "/" + document));
				}
			}
		).reduceByKey(
			/************ YOUR CODE HERE ************/
			new Function2<String, String, String>() {
				public String call(String v1, String v2) {
					StringBuilder stringBuilder = new StringBuilder();

					String[] countSlashDocumentV1 = v1.split("/");
					String[] countSlashDocumentV2 = v2.split("/");
					int countV1 = Integer.parseInt(countSlashDocumentV1[0]);
					int countV2 = Integer.parseInt(countSlashDocumentV2[0]);
					int currentNumDocsWithWord = countV1 + countV2;

					String documentV1 = countSlashDocumentV1[1];
					String documentV2 = countSlashDocumentV2[1];

					stringBuilder.append(documentV1);
					stringBuilder.append(",");
					stringBuilder.append(documentV2);

					String numDocsWithWordAndAllDocuments = currentNumDocsWithWord + "/" + stringBuilder.toString();
					return numDocsWithWordAndAllDocuments;
				}
			}

		).flatMapToPair(
			/************ YOUR CODE HERE ************/
			new PairFlatMapFunction <Tuple2<String,String>,String,String>() {
				public Iterable<Tuple2<String, String>> call(Tuple2<String, String> wordWithNumDocsWithWordAndAllDocuments) {
					List<Tuple2<String, String>> mapOutputList = new ArrayList<>();

					String word = wordWithNumDocsWithWordAndAllDocuments._1;
					String numDocsWithWordAndAllDocuments = wordWithNumDocsWithWordAndAllDocuments._2;
					String[] numDocsWithWordAndAllDocumentsSplitted = numDocsWithWordAndAllDocuments.split("/");
					String numDocsWithWord = numDocsWithWordAndAllDocumentsSplitted[0];
					// All documents each spearated by ","
					String documents = numDocsWithWordAndAllDocumentsSplitted[1];
					// Constructing value to be emitted (numDocs/numDocsWithWord).
					// Will be same for the same word in all documents
					String numDocsAndNumDocsWithWord = numDocs + "/" + numDocsWithWord;
					// Splitting documents separated by "," so as to get each individual document name
					String[] documentsSplitted = documents.split(",");
					for(String document : documentsSplitted) {
						String wordAtDocument = word + "@" + document;
						mapOutputList.add(new Tuple2(wordAtDocument, numDocsAndNumDocsWithWord));
					}
					return mapOutputList;
				}
			}

		);

		//Print idfRDD contents
		if (DEBUG) {
			List<Tuple2<String, String>> list = idfRDD.collect();
			System.out.println("-------Contents of idfRDD-------");
			for (Tuple2<String, String> tuple : list) {
				System.out.println("(" + tuple._1 + ") , (" + tuple._2 + ")");
			}
			System.out.println("--------------------------------");
		}

		/*
		 * TF * IDF Job
		 * Calculates final TFIDF value from tfRDD and idfRDD
		 *
		 * Input:  ( (word@document) , (wordCount/docSize) )          [from tfRDD]
		 * Map:    ( (word@document) , TF )
		 *
		 * Input:  ( (word@document) , (numDocs/numDocsWithWord) )    [from idfRDD]
		 * Map:    ( (word@document) , IDF )
		 *
		 * Union:  ( (word@document) , TF )  U  ( (word@document) , IDF )
		 * Reduce: ( (word@document) , TFIDF )
		 * Map:    ( (document@word) , TFIDF )
		 *
		 * where TF    = wordCount/docSize
		 * where IDF   = ln(numDocs/numDocsWithWord)
		 * where TFIDF = TF * IDF
		 */
		JavaPairRDD<String,Double> tfFinalRDD = tfRDD.mapToPair(
			new PairFunction<Tuple2<String,String>,String,Double>() {
				public Tuple2<String,Double> call(Tuple2<String,String> x) {
					double wordCount = Double.parseDouble(x._2.split("/")[0]);
					double docSize = Double.parseDouble(x._2.split("/")[1]);
					double TF = wordCount/docSize;
					// Emitting (word@document) and (TF i.e term frequency)
					return new Tuple2(x._1, TF);
				}
			}
		);

		JavaPairRDD<String,Double> idfFinalRDD = idfRDD.mapToPair(
			/************ YOUR CODE HERE ************/
			new PairFunction<Tuple2<String, String>, String, Double>() {
				public Tuple2<String, Double> call(Tuple2<String, String> x) {
					double numDocsWithWord = Double.parseDouble(x._2.split("/")[1]);
					double IDF = Math.log(numDocs/numDocsWithWord);
					// Emitting (word@document) and (IDF i.e inverse document frequency)
					return new Tuple2(x._1, IDF);
				}
			}

		);

		JavaPairRDD<String,Double> tfidfRDD = tfFinalRDD.union(idfFinalRDD).reduceByKey(
			/************ YOUR CODE HERE ************/
			new Function2<Double, Double, Double>() {
				public Double call(Double TF, Double IDF) {
					return TF * IDF;
				}
			}
		).mapToPair(
			/************ YOUR CODE HERE ************/
			new PairFunction<Tuple2<String, Double>, String, Double>() {
				public Tuple2<String, Double> call(Tuple2<String, Double> wordAtDocumentAndTFIDF) {
					String wordAtDocument = wordAtDocumentAndTFIDF._1;
					// Just reversing and emitting (document@word) and (TFIDF)
					return new Tuple2(wordAtDocument.split("@")[1] + "@" + wordAtDocument.split("@")[0], wordAtDocumentAndTFIDF._2);
				}
			}
		);

		//Print tfidfRDD contents in sorted order
		Map<String, Double> sortedMap = new TreeMap<>();
		List<Tuple2<String, Double>> list = tfidfRDD.collect();
		for (Tuple2<String, Double> tuple : list) {
			sortedMap.put(tuple._1, tuple._2);
		}
		if(DEBUG) System.out.println("-------Contents of tfidfRDD-------");
		for (String key : sortedMap.keySet()) {
			System.out.println(key + "\t" + sortedMap.get(key));
		}
		if(DEBUG) System.out.println("--------------------------------");
	}
}
