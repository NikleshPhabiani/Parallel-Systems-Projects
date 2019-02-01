/*
Single Author Info:
nphabia Niklesh Ashok Phabiani
*/
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.*;

/*
 * Main class of the TFIDF MapReduce implementation.
 * Author: Tyler Stocksdale
 * Date:   10/18/2017
 */
public class TFIDF {

    public static void main(String[] args) throws Exception {
        // Check for correct usage
        if (args.length != 1) {
            System.err.println("Usage: TFIDF <input dir>");
            System.exit(1);
        }

		    // Create configuration
		    Configuration conf = new Configuration();

    		// Input and output paths for each job
    		Path inputPath = new Path(args[0]);
    		Path wcInputPath = inputPath;
    		Path wcOutputPath = new Path("output/WordCount");
    		Path dsInputPath = wcOutputPath;
    		Path dsOutputPath = new Path("output/DocSize");
    		Path tfidfInputPath = dsOutputPath;
    		Path tfidfOutputPath = new Path("output/TFIDF");

    		// Get/set the number of documents (to be used in the TFIDF MapReduce job)
        FileSystem fs = inputPath.getFileSystem(conf);
        FileStatus[] stat = fs.listStatus(inputPath);
    		String numDocs = String.valueOf(stat.length);
    		conf.set("numDocs", numDocs);

    		// Delete output paths if they exist
    		FileSystem hdfs = FileSystem.get(conf);
    		if (hdfs.exists(wcOutputPath))
    			hdfs.delete(wcOutputPath, true);
    		if (hdfs.exists(dsOutputPath))
    			hdfs.delete(dsOutputPath, true);
    		if (hdfs.exists(tfidfOutputPath))
    			hdfs.delete(tfidfOutputPath, true);

    		// Create and execute Word Count job
        Job wordCountJob = Job.getInstance(conf, "Word count job instance");
        wordCountJob.setJarByClass(TFIDF.class);
        wordCountJob.setMapperClass(WCMapper.class);
        wordCountJob.setReducerClass(WCReducer.class);
        wordCountJob.setOutputKeyClass(Text.class);
        wordCountJob.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(wordCountJob, wcInputPath);
        FileOutputFormat.setOutputPath(wordCountJob, wcOutputPath);
        if(!wordCountJob.waitForCompletion(true)) {
          throw new InterruptedException("Word count job failed");
        }

    		// Create and execute Document Size job
        Job docSizeJob = Job.getInstance(conf, "Document size job instance");
        docSizeJob.setJarByClass(TFIDF.class);
        docSizeJob.setMapperClass(DSMapper.class);
        docSizeJob.setReducerClass(DSReducer.class);
        docSizeJob.setOutputKeyClass(Text.class);
        docSizeJob.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(docSizeJob, dsInputPath);
        FileOutputFormat.setOutputPath(docSizeJob, dsOutputPath);
        if(!docSizeJob.waitForCompletion(true)) {
          throw new InterruptedException("Document size job failed");
        }

    		//Create and execute TFIDF job
        Job tfidfJob =  Job.getInstance(conf, "TFIDF job instance");
        tfidfJob.setJarByClass(TFIDF.class);
        tfidfJob.setMapperClass(TFIDFMapper.class);
        tfidfJob.setReducerClass(TFIDFReducer.class);
        tfidfJob.setOutputKeyClass(Text.class);
        tfidfJob.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(tfidfJob, tfidfInputPath);
        FileOutputFormat.setOutputPath(tfidfJob, tfidfOutputPath);
        if(!tfidfJob.waitForCompletion(true)) {
          throw new InterruptedException("TFIDF job failed");
        }
    }

	/*
	 * Creates a (key,value) pair for every word in the document
	 *
	 * Input:  ( byte offset , contents of one line )
	 * Output: ( (word@document) , 1 )
	 *
	 * word = an individual word in the document
	 * document = the filename of the document
	 */
	public static class WCMapper extends Mapper<Object, Text, Text, IntWritable> {
    // Intermediate key in appropriate format
    private Text intermediateKey = new Text();
    // Intermediate value in appropriate required format
    private final static IntWritable valueOne = new IntWritable(1);

    // map method processes one line at a time
    // For each word in the line, the map function emits a key-value pair of <<word@docName>, 1>
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String valueAsString = value.toString();
      String[] words = valueAsString.split("\\s+");
      String docName = ((FileSplit) context.getInputSplit()).getPath().getName();
      for(String word : words) {
        String wordAtDocument = word + "@" + docName;
        context.write(new Text(wordAtDocument), valueOne);
      }
    }
  }

    /*
	 * For each identical key (word@document), reduces the values (1) into a sum (wordCount)
	 *
	 * Input:  ( (word@document) , 1 )
	 * Output: ( (word@document) , wordCount )
	 *
	 * wordCount = number of times word appears in document
	 */
	public static class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable totalWordCount = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int count = 0;
      // Reducing the count for each word in a single document
      for(IntWritable value : values) {
        count += value.get();
      }
      totalWordCount.set(count);
      context.write(key, totalWordCount);
    }

  }

	/*
	 * Rearranges the (key,value) pairs to have only the document as the key
	 *
	 * Input:  ( (word@document) , wordCount )
	 * Output: ( document , (word=wordCount) )
	 */
	public static class DSMapper extends Mapper<Object, Text, Text, Text> {
    // At used in variable name represents "@"
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] keyAndValueSplitted = value.toString().split("\\s+");
      // Storing (word@document) and (wordCount)
      String wordAtDocument = keyAndValueSplitted[0];
      String wordCount = keyAndValueSplitted[1];

      String[] wordAndDocumentSplitted = wordAtDocument.split("@");
      // Storing (word) and (document)
      String word = wordAndDocumentSplitted[0];
      String document = wordAndDocumentSplitted[1];
      // Making the new (value) that needs to be made a part of the output
      String wordWithWordCount = word + "=" + wordCount;
      // Converting key and value to appropriate format (Text in our case),
      // so as to do a context.write using these
      Text docName = new Text(document);
      Text wordWithCount = new Text(wordWithWordCount);
      //docName.set(document);
      //wordWithCount.set(word + "=" + wordCount);
      context.write(docName, wordWithCount);
    }
  }

    /*
	 * For each identical key (document), reduces the values (word=wordCount) into a sum (docSize)
	 *
	 * Input:  ( document , (word=wordCount) )
	 * Output: ( (word@document) , (wordCount/docSize) )
	 *
	 * docSize = total number of words in the document
	 */
	public static class DSReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text document, Iterable<Text> wordWithCountList, Context context) throws IOException, InterruptedException {
      int docSize = 0;
      int iterator = 0;
      List<String> wordsInDoc = new ArrayList<>();
      List<String> wordCount = new ArrayList<>();

      String docName = document.toString();

      // Calculating document size
      for(Text wordWithCount : wordWithCountList) {
        String wordWithCountAsString = wordWithCount.toString();
        String[] wordAndCountSplitted = wordWithCountAsString.split("=");
        String word = wordAndCountSplitted[0];
        String count = wordAndCountSplitted[1];

        docSize += Integer.parseInt(count);
        wordsInDoc.add(word);
        wordCount.add(count);
      }
      // Calculating term frequency and emitting it as key-value pair of <<word@document>, termFrequency>
      for(String word : wordsInDoc) {
        String wordWithDocument = word + "@" + docName;
        String tf = wordCount.get(iterator) + "/" + docSize;
        Text wordAtDocument = new Text(wordWithDocument);
        Text termFrequency = new Text(tf);
        // wordAtDocument.set(word + "@" + docName);
        // termFrequency.set(wordCount.get(iterator) + "/" + docSize);
        iterator++;
        context.write(wordAtDocument, termFrequency);
      }
    }
  }

	/*
	 * Rearranges the (key,value) pairs to have only the word as the key
	 *
	 * Input:  ( (word@document) , (wordCount/docSize) )
	 * Output: ( word , (document=wordCount/docSize) )
	 */
	public static class TFIDFMapper extends Mapper<Object, Text, Text, Text> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] keyAndValueSplitted = value.toString().split("\\s+");
      // Retrieving (word@document) and (wordCount/docSize)
      String wordAtDocument = keyAndValueSplitted[0];
      String termFrequency = keyAndValueSplitted[1];

      String[] wordAndDocumentSplitted = wordAtDocument.split("@");
      // Retrieving (word) and (document)
      String word = wordAndDocumentSplitted[0];
      String document = wordAndDocumentSplitted[1];

      // Creating intermediate value (document=wordCount/docSize) that would then
      // get written along with the intermediate key (word) in the intermediate file system
      String documentAndTermFrequency = document + "=" + termFrequency;
      context.write(new Text(word), new Text(documentAndTermFrequency));
    }
  }

    /*
	 * For each identical key (word), reduces the values (document=wordCount/docSize) into a
	 * the final TFIDF value (TFIDF). Along the way, calculates the total number of documents and
	 * the number of documents that contain the word.
	 *
	 * Input:  ( word , (document=wordCount/docSize) )
	 * Output: ( (document@word) , TFIDF )
	 *
	 * numDocs = total number of documents
	 * numDocsWithWord = number of documents containing word
	 * TFIDF = (wordCount/docSize) * ln(numDocs/numDocsWithWord)
	 *
	 * Note: The output (key,value) pairs are sorted using TreeMap ONLY for grading purposes. For
	 *       extremely large datasets, having a for loop iterate through all the (key,value) pairs
	 *       is highly inefficient!
	 */
	public static class TFIDFReducer extends Reducer<Text, Text, Text, Text> {

  		private static int numDocs;
  		private Map<Text, Text> tfidfMap = new HashMap<>();

  		// gets the numDocs value and stores it
  		protected void setup(Context context) throws IOException, InterruptedException {
        	Configuration conf = context.getConfiguration();
        	numDocs = Integer.parseInt(conf.get("numDocs"));
  		}

  		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int numDocsWithWord = 0;
        // Retrieving (word), which comes as key from the TFIDF mapper
        String word = key.toString();
        List<String> documentAndTermFrequencyList = new ArrayList<>();

        for(Text value : values) {
          String documentAndTermFrequency = value.toString();
          documentAndTermFrequencyList.add(documentAndTermFrequency);
          numDocsWithWord++;
        }

        for(String documentAndTermFrequency : documentAndTermFrequencyList) {
          double tfidf = 0;
          String[] documentAndTermFrequencySplitted = documentAndTermFrequency.split("=");

          String document = documentAndTermFrequencySplitted[0];
          String termFrequency = documentAndTermFrequencySplitted[1];

          String[] tfNumAndDen = termFrequency.split("/");
          int tfNumerator = Integer.parseInt(tfNumAndDen[0]);
          int tfDenominator = Integer.parseInt(tfNumAndDen[1]);
          double termFrequencyAsDouble = (double)(tfNumerator) / tfDenominator;

          // Calculating TFIDF
          tfidf = termFrequencyAsDouble * Math.log((double)(numDocs)/(double)(numDocsWithWord));
          String documentAndWord = document + "@" + word;
          // Putting the output (key,value) pair into the tfidfMap instead of doing a context.write
          tfidfMap.put(new Text(documentAndWord), new Text("" + tfidf));
        }
  		}

  		// sorts the output (key,value) pairs that are contained in the tfidfMap
  		protected void cleanup(Context context) throws IOException, InterruptedException {
          Map<Text, Text> sortedMap = new TreeMap<Text, Text>(tfidfMap);
  		    for (Text key : sortedMap.keySet()) {
              context.write(key, sortedMap.get(key));
          }
      }
  }
}
