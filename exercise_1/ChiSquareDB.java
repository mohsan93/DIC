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

public class ChiSquareDB {

  public static HashMap<String, Integer> categoryCounts = new HashMap<String, Integer>();
  private static final Log LOG = LogFactory.getLog(ChiSquareDB.class);
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{
    
    

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      
      String[] raw = value.toString().split("\\s+");
      String[] termCat = raw[0].split("@");
      //Integer count = Integer.parseInt(raw[1].trim());
      context.write(new Text(termCat[0].trim()), new Text(termCat[1] + "$$" + raw[1]));
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {

   
    
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      
      
      HashMap<String, String> termCounts = new HashMap<String, String>();
      int i = 0;
      /*String t_string;
      for (Text t : values){
        t_string = t.toString();
        t_string = t_string.substring(1, t_string.length() - 1);

        String[] valueParts = t_string.split(",");
        for (String v : valueParts){
          //String[] splits = v.split("=");
          termCounts.put("nope_" + Integer.toString(i), v);
          i += 1;
        }
      }*/
      String val;
      String total = "";
      for (Text t : values){
        val = t.toString();
        total += val + ",";
        //termCounts.put(val.split("$woop$")[0], val.split("$woop$")[1]);
      }
      context.write(key, new Text(total.substring(0, total.length() - 1)));

      /*
      boolean checker = false;
      for (Text t : values){
        String[] valueParts = t.toString().split("\\:");
        if (valueParts.length == 2){
        termCounts.put(valueParts[0].trim(), Integer.parseInt(valueParts[1]));
        }
        else{
          checker = true;
        }
      }

      if (checker){
        for (Text t : values){
        context.write(key, t);
        }
      }
      else{
      context.write(key, new Text(termCounts.toString()));
      } */ 

      //Calculating Chi Square:
      /*int a = 0;
      int b = 0;
      int c = 0;
      int d = 0;
      int chisq = 0;

      for ( String keyG : categoryCounts.keySet() ) {
        if (termCounts.containsKey(keyG)){
          a = termCounts.get(keyG);
        }
        for (String keyB : termCounts.keySet()){
          if (keyB != keyG){
            b += termCounts.get(keyB);
          }
        }
        c = categoryCounts.get(keyG) - a;

        for (String keyD : categoryCounts.keySet()){
          if (keyD != keyG){
            if (termCounts.containsKey(keyD)){
              d += categoryCounts.get(keyD) - termCounts.get(keyD);
            }
            else{
              d += categoryCounts.get(keyD);
            }
          }
        }
        chisq = (int) Math.round((Math.pow((a*d-b*c), 2))/((a+b)*(a+c)*(b+d)*(c+d)));
        a = 0;
        b = 0;
        c = 0;
        d = 0;
        LOG.error(chisq);
        LOG.error(termCounts);
        context.write(new Text(key.toString() + "@" + keyG), new IntWritable(chisq));
      } */
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "ChiSquare");
    FileSystem fileSystem = FileSystem.get(conf);

    job.setNumReduceTasks(2);
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

    Path hdfsReadPath = new Path(args[2]);
    FSDataInputStream inputStream = fileSystem.open(hdfsReadPath);

    BufferedReader bufferedReader = new BufferedReader(
                new InputStreamReader(inputStream, StandardCharsets.UTF_8));

    String line = null;
    while ((line=bufferedReader.readLine())!=null){
        String [] categoryC = line.split("\\s+");
        categoryCounts.put(categoryC[0].trim(), Integer.parseInt(categoryC[1]));
        System.out.println(categoryC[0].trim());
    }

    System.out.println("categories:\n");
    System.out.println(categoryCounts);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
