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

  //Input format: term  category$$count,category$$count...

  //private static final Log LOG = LogFactory.getLog(ChiSquare.class);
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
        //Splitting up chisq calculation to make sure that double values are used (not Double):
        //long upper = Math.round(Math.pow((a*d-b*c), 2));
        //long lower = ((a+b)*(a+c)*(b+d)*(c+d));
        //long upper = Math.pow((a*d-b*c), 2);

        /*if (a >= 0 && b >= 0 && c >= 0 && d >= 0){
          
          long upper = 0;
          long lower = 0;
          long step1 = (b+d) * (c+d);
          if (((b+d) != step1 / (c+d))){
            step1 = Long.MAX_VALUE;
          }
          long step2 = (a+b) * (a+c);
          if (((a+b) != step2 / (a+c))){
            step2 = Long.MAX_VALUE;
          }

          long step3 = 0;
          if (step1 == Long.MAX_VALUE && (step2 > 1 ) || step2 == Long.MAX_VALUE && (step1 > 1 )){
            step3 = Long.MAX_VALUE;
          }
          else{
            step3 = step1*step2;
          }


          long step4 = ((a*d)-(b*c)) * ((a*d)-(b*c));
          if (((a*d)-(b*c)) == step4/((a*d)-(b*c))){
            chisq = step4/step3;
          }
          else{
            if (step3 > 1){
            chisq = Long.MAX_VALUE / step3;
            }
            else{
              chisq = Long.MAX_VALUE;
            }
          }
        }
        else{
          chisq = -1;
        }*/

        chisq = ((a*d-b*c)*(a*d-b*c)) / ((a+b)*(a+c)*(b+d)*(c+d));
        context.write(new Text(raw[0] + "@" + keyG), new Text(Double.toString(chisq))); //new Text(Long.toString(chisq)));
        
        //contex.write(new Text(raw[0] + "@" + keyG), new LongWritable(chisq));
        
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
    job.setMapOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

   
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
