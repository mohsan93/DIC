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

public class ChiSquareDB {

  //Input format: term  category$$count,category$$count...

  //private static final Log LOG = LogFactory.getLog(ChiSquare.class);
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{
    
    

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      
      HashMap<String, Long> termCounts = new HashMap<String, Long>();
      HashMap<String, Long> allCategories = new HashMap<String, Long>();
      String[] raw = value.toString().split("\\s+");
      String[] categoriesAndCounts = raw[1].split(",");
      


      for (String part : categoriesAndCounts){
        termCounts.put(part.split("\\$\\$")[0], Long.parseLong(part.split("\\$\\$")[1]));
      }

      Configuration config = context.getConfiguration();
      String categoryCounts = config.get("categoryCounts");
      for (String val : categoryCounts.split(",")){
        String [] cat_count = val.split("\\s+");
        allCategories.put(cat_count[0], Long.parseLong(cat_count[1]));
      }
      //Calculating Chi Square:
      long a = 0;
      long b = 0;
      long c = 0;
      long d = 0;
      long chisq = 0;

      //context.write(new Text(categoryCounts), new IntWritable(1));

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

        //chisq = (long) Math.round((Math.pow((a*d-b*c), 2))/((a+b)*(a+c)*(b+d)*(c+d)));
        //Splitting up chisq calculation to make sure that double values are used (not float):
        //long upper = Math.round(Math.pow((a*d-b*c), 2));
        //long lower = ((a+b)*(a+c)*(b+d)*(c+d));
        //chisq = Math.round(upper/lower);

        //context.write(new Text(raw[0] + "@" + keyG), new Text("a:"+Long.toString(a) + " ," + "b:"+Long.toString(b) + " ," +"c:"+Long.toString(c) + " ," +"a:"+Long.toString(d) + " ,"));
        context.write(new Text(raw[0] + "@" + keyG), new Text(termCounts + " |||  all cats: " + allCategories));
        a = 0;
        b = 0;
        c = 0;
        d = 0;
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

    
    Job job = Job.getInstance(conf, "ChiSquareDB");
    

    job.setNumReduceTasks(0);
    job.setJarByClass(ChiSquareDB.class);
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
