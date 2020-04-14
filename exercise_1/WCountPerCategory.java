import java.io.IOException;
import java.util.StringTokenizer;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import java.util.*; 
import java.lang.Math;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import java.io.BufferedReader;
import org.apache.hadoop.fs.FSDataInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

public class WCountPerCategory {
  /***************************************
  This MapReduce Job is used to count in how many documents a term appears, per category.
  This requires filtering out multiple occurences of terms in the same document.
  The Stopword list is also imported via the buffer reader and made available for the mappers
  via the context.
  Here i assume that reviewerID+asin forms a unique document ID.
   **************************************/

  //The map function, outputs <(term$category), (reviewerID+asin)>
  //standard input type for reading a text/json file, all Text.
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

    private final static IntWritable ONE = new IntWritable(1);
    private Text word = new Text();
    private final JSONParser parser = new JSONParser();
    
    //map function
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      
      //create arraylist for stopwords
      ArrayList<String> stopwords = new ArrayList<String>();

      //get the stopwordslist from the configuration and 
      //add it to ArrayList
      Configuration config = context.getConfiguration();
      String stopwordsCompact = config.get("stopwords");
      for (String val : stopwordsCompact.split(",")){
        stopwords.add(val);
      }

      //parse the json file
      JSONObject jsonObj;

      try {
      jsonObj = (JSONObject) parser.parse(value.toString());
      }
      catch (ParseException e){
        e.printStackTrace();
        return;
      }
      //get the category
      String category = jsonObj.get("category").toString();
      category = category.replace("\"", "").trim();

      //get the other values from the json file
      String review = jsonObj.get("reviewText").toString();
      String reviewerID = jsonObj.get("reviewerID").toString();
      String asin = jsonObj.get("asin").toString();

      //tokenize and filter the strings and add it to the context
      StringTokenizer itr = new StringTokenizer(review, " .!?,;:<>()[]{}-_\"`+~#&*%$");

      String term;
      while (itr.hasMoreTokens()) {
        String token = itr.nextToken();
        //split on any numbers
        for (String t : token.split("[0-9]+")){
          //terms with length == 1 are discarded
          if (t.length() > 1){
            term = t.toLowerCase();
            //if term is in stopwords list, it is discarded
            if (!stopwords.contains(term)){
              //reviewTerms.add(tmp);
              word.set(term + "$" + category);
              context.write(word, new Text(reviewerID + asin));
            }
          }
        }
      }  
    }
  }

  //Combiner, uses a hashset at each mapper node to filter out duplicate documents/values
  //Input is all Text, so is the output
  public static class IntSumCombiner
       extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      
      HashSet<String> uniqueDocs = new HashSet<String>(); 
      for (Text val : values) {
        uniqueDocs.add(val.toString());
      }
      //duplicate free values are written back to the context
     Iterator<String> it = uniqueDocs.iterator();
     while(it.hasNext()){
       context.write(key, new Text(it.next()));
     }
    }
  }

  //Reducer does basically the same as the combiner, output value is 
  //the number of unique documents per term per category (=count of unique reviewerID+asin strings)
  public static class IntSumReducer
       extends Reducer<Text,Text,Text,LongWritable> {

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      
      //filter out duplicate reviewerID+asin values, we only want unique values
      HashSet<String> uniqueDocs = new HashSet<String>(); 
      for (Text val : values) {
        uniqueDocs.add(val.toString());
      }
      
      //write <(term$category),(CountOfDocuments)> to file 
      context.write(key, new LongWritable(uniqueDocs.size()));
    }
  }

  public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();

    //the following code is used to read the stopwords file
    //from the hdfs system into the context to be made available
    //for all mappers.

    FileSystem fileSystem = FileSystem.get(conf);

    Path hdfsReadPath = new Path(args[2]);
    FSDataInputStream inputStream = fileSystem.open(hdfsReadPath);

    BufferedReader bufferedReader = new BufferedReader(
                new InputStreamReader(inputStream, StandardCharsets.UTF_8));

    //read the stopwords list into one long comma separated string.
    String line = null;
    String stopwords = "";
    while ((line=bufferedReader.readLine())!=null){
        stopwords += line + ",";
    }
    stopwords = stopwords.substring(0, stopwords.length() -1);

    //add stopwordlist to context.
    conf.set("stopwords", stopwords);


    Job job = Job.getInstance(conf, "WCountPerCategory");
    job.setNumReduceTasks(8);
    job.setJarByClass(WCountPerCategory.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumCombiner.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    //List<String> swords = new ArrayList<String>();
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
