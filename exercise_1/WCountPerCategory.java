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

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

    private final static IntWritable ONE = new IntWritable(1);
    private Text word = new Text();
    private final JSONParser parser = new JSONParser();
    

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      
      ArrayList<String> stopwords = new ArrayList<String>();

      Configuration config = context.getConfiguration();
      String stopwordsCompact = config.get("stopwords");
      for (String val : stopwordsCompact.split(",")){
        stopwords.add(val);
      }


      JSONObject jsonObj;

      try {
      jsonObj = (JSONObject) parser.parse(value.toString());
      }
      catch (ParseException e){
        e.printStackTrace();
        return;
      }
      String category = jsonObj.get("category").toString();
      category = category.replace("\"", "").trim();

      String review = jsonObj.get("reviewText").toString();
      String reviewerID = jsonObj.get("reviewerID").toString();
      String asin = jsonObj.get("asin").toString();


      StringTokenizer itr = new StringTokenizer(review, " .!?,;:<>()[]{}-_\"`+~#&*%$");

      String term;
      while (itr.hasMoreTokens()) {
        String token = itr.nextToken();
        for (String t : token.split("[0-9]+")){
          if (t.length() > 1){
            term = t.toLowerCase();
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


  public static class IntSumCombiner
       extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      
      HashSet<String> uniqueDocs = new HashSet<String>(); 
      for (Text val : values) {
        uniqueDocs.add(val.toString());
      }
    
     Iterator<String> it = uniqueDocs.iterator();
     while(it.hasNext()){
       context.write(key, new Text(it.next()));
     }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,LongWritable> {

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      
      HashSet<String> uniqueDocs = new HashSet<String>(); 
      for (Text val : values) {
        uniqueDocs.add(val.toString());
      }
      context.write(key, new LongWritable(uniqueDocs.size()));
    }
  }

  public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();
    FileSystem fileSystem = FileSystem.get(conf);

    Path hdfsReadPath = new Path(args[2]);
    FSDataInputStream inputStream = fileSystem.open(hdfsReadPath);

    BufferedReader bufferedReader = new BufferedReader(
                new InputStreamReader(inputStream, StandardCharsets.UTF_8));

    String line = null;
    String stopwords = "";
    while ((line=bufferedReader.readLine())!=null){
        stopwords += line + ",";
    }
    stopwords = stopwords.substring(0, stopwords.length() -1);
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
