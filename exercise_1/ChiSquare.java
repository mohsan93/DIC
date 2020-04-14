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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ChiSquare {
  /***************************************
  This MapReduce Job is used to calculate the ChiSquare Value for each term of every category.
  The input for this job comes from ChiSquarePrepro.java and looks like this: <term, (category$$count, category$$count,...)>.
  Here only the mapper is used, no reducer is needed.
  First, the total number of documents for each category are loaded from hdfs in the main method and written to the context
  configuration to be later retrieved by each mapper. It is saved to the hashmap variable allCategories.
  Next, in the mapper, for each term (key), the comma separated value line is split by $$ and put into a hashmap, like so:
  HashMap<String, Double> termCounts = HashMap<Category, Count>.
  The calculation of the ChiSquare is then straight forward, a, b, c and d (see slides) are calculated via the two hashmaps.
   **************************************/
  

  //Input format: term  category$$count,category$$count...
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{
    
    

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      
      HashMap<String, Double> termCounts = new HashMap<String, Double>();
      HashMap<String, Double> allCategories = new HashMap<String, Double>();
      String[] raw = value.toString().split("\\s+");
      String[] categoriesAndCounts = raw[1].split(",");
      


      for (String part : categoriesAndCounts){
        termCounts.put(part.split("\\$\\$")[0], Double.parseDouble(part.split("\\$\\$")[1]));
      }

      //Get the categoryCounts hashmap from the context configuration
      Configuration config = context.getConfiguration();
      String categoryCounts = config.get("categoryCounts");
      for (String val : categoryCounts.split(",")){
        String [] cat_count = val.split("\\s+");
        allCategories.put(cat_count[0], Double.parseDouble(cat_count[1]));
      }
      //Calculating Chi Square:
      double a = 0.0;
      double b = 0.0;
      Double c = 0.0;
      double d = 0.0;
      double chisq = 0.0;

      //a, b, c and d are calculated as described in the slides
      //using the two hashmaps categoryCounts and termCounts, the first one
      //containing the number of documents per category, the second one containing
      //the number of documents per category for a single term.

      for ( String keyG : allCategories.keySet() ) {
        if (termCounts.containsKey(keyG)){
          a = termCounts.get(keyG);
        }
        for (String keyB : termCounts.keySet()){
          if (!keyB.equals(keyG)){
            b += termCounts.get(keyB);
          }
        }
        c = allCategories.get(keyG) - a;


        for (String keyD : allCategories.keySet()){
          if (!keyD.equals(keyG)){
            d += allCategories.get(keyD);
            if (termCounts.containsKey(keyD)){
              d -= termCounts.get(keyD);
            }
          }
        }

        //numerical calculation of the chisquare value.
        chisq = ((a*d-b*c)*(a*d-b*c)) / ((a+b)*(a+c)*(b+d)*(c+d));

        //Outputs <(term@category), ChiSquare Value> key value pairs.
        context.write(new Text(raw[0] + "@" + keyG), new Text(Double.toString(chisq)));
        
        //resetting the values after each loop
        a = 0.0;
        b = 0.0;
        c = 0.0;
        d = 0.0;
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {

   
    
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      
      for (Text v : values){
        context.write(key, v);    
      }
    }
  }

  public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();

    //creating string variable to contain the categoryCounts.
    String categoryCounts = "";
    //preamble to connect to hadoop file system and read the file via BufferedReader.
    FileSystem fileSystem = FileSystem.get(conf);

    Path hdfsReadPath = new Path(args[2]);
    FSDataInputStream inputStream = fileSystem.open(hdfsReadPath);

    BufferedReader bufferedReader = new BufferedReader(
                new InputStreamReader(inputStream, StandardCharsets.UTF_8));

    String line = null;
    
    while ((line=bufferedReader.readLine())!=null){
        categoryCounts += line + ",";
    }
    categoryCounts = categoryCounts.substring(0, categoryCounts.length() -1);
    //write a comma separated string (category  count, category  count, ...)
    //to the context
    conf.set("categoryCounts", categoryCounts);

    
    Job job = Job.getInstance(conf, "ChiSquare");
    
    //number of reducer tasks set to 0 because we dont need reducers for this one.
    job.setNumReduceTasks(0);
    job.setJarByClass(ChiSquare.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

   
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
