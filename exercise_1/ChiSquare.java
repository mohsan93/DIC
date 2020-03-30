import java.io.IOException;
import java.util.StringTokenizer;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

  //Input format: term  category$$count,category$$count...

  private static final Log LOG = LogFactory.getLog(ChiSquare.class);
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{
    
    

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      
      HashMap<String, Integer> termCounts = new HashMap<String, Integer>();
      HashMap<String, Integer> allCategories = new HashMap<String, Integer>();
      String[] raw = value.toString().split("\\s+");
      String[] categoriesAndCounts = raw[1].split(",");
      


      for (String part : categoriesAndCounts){
        termCounts.put(part.split("\\$\\$")[0], Integer.parseInt(part.split("\\$\\$")[1]));
      }

      Configuration config = context.getConfiguration();
      String categoryCounts = config.get("categoryCounts");
      for (String val : categoryCounts.split(",")){
        String [] cat_count = val.split("\\s+");
        allCategories.put(cat_count[0], Integer.parseInt(cat_count[1]));
      }
      //Calculating Chi Square:
      int a = 0;
      int b = 0;
      int c = 0;
      int d = 0;
      int chisq = 0;

      //context.write(new Text(categoryCounts), new IntWritable(1));

      for ( String keyG : allCategories.keySet() ) {
        if (termCounts.containsKey(keyG)){
          a = termCounts.get(keyG);
        }
        for (String keyB : termCounts.keySet()){
          if (keyB != keyG){
            b += termCounts.get(keyB);
          }
        }
        c = allCategories.get(keyG) - a;

        for (String keyD : allCategories.keySet()){
          if (keyD != keyG){
            if (termCounts.containsKey(keyD)){
              d += allCategories.get(keyD) - termCounts.get(keyD);
            }
            else{
              d += allCategories.get(keyD);
            }
          }
        }

        chisq = (int) Math.round((Math.pow((a*d-b*c), 2))/((a+b)*(a+c)*(b+d)*(c+d)));
        a = 0;
        b = 0;
        c = 0;
        d = 0;
        context.write(new Text(raw[0] + "@" + keyG), new IntWritable(chisq));
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {

   
    
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      
      
      HashMap<String, String> termCounts = new HashMap<String, String>();
      int i = 0;
     
      String val;
      String total = "";
      for (Text t : values){
        val = t.toString();
        total += val + ",";
        //termCounts.put(val.split("$woop$")[0], val.split("$woop$")[1]);
      }
      context.write(key, new Text(total.substring(0, total.length() - 1)));    
    }
  }

  public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();
    String categoryCounts = "";
    FileSystem fileSystem = FileSystem.get(conf);

    Path hdfsReadPath = new Path(args[2]);
    FSDataInputStream inputStream = fileSystem.open(hdfsReadPath);

    BufferedReader bufferedReader = new BufferedReader(
                new InputStreamReader(inputStream, StandardCharsets.UTF_8));

    String line = null;
    
    while ((line=bufferedReader.readLine())!=null){
        //String [] categoryC = line.split("\\s+");
        categoryCounts += line + ",";
    }
    categoryCounts = categoryCounts.substring(0, categoryCounts.length() -1);
    conf.set("categoryCounts", categoryCounts);

    
    Job job = Job.getInstance(conf, "ChiSquare");
    

    job.setNumReduceTasks(0);
    job.setJarByClass(ChiSquare.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

   
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
