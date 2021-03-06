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

public class ChiSquarePrepro {
  /***************************************
  This MapReduce Job is an interim job, kind of a preprocessing step for the acutal ChiSquare job.
  What does it do? The WCountPerCategory.java MapReduce Job outputs <(term$category),counts> key-value
  pairs. For easier processing, i want the output to look like this: <term, (category$$count, category$$count,...)>.
  So the count of unique documents for each category the term exists in are written in the same line, separated by comma.
  It outputs one line for each term.
   **************************************/

  //Mapper, standard setting, takes on Text and outputs Text.
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{
    
    

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      
      String[] raw = value.toString().split("\\s+");
      String[] termCat = raw[0].split("\\$");
      //Integer count = Integer.parseInt(raw[1].trim());
      if (termCat.length > 1 && raw.length > 1){
        context.write(new Text(termCat[0].trim()), new Text(termCat[1] + "$$" + raw[1]));
      }
    }
  }

  //Here the reducer is used to write all values of the same key (term) into one comma separated line.
  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {

   
    
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
     
      String val;
      String total = "";
      for (Text t : values){
        val = t.toString();
        total += val + ",";
      }
      context.write(key, new Text(total.substring(0, total.length() - 1)));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "ChiSquarePrepro");
    FileSystem fileSystem = FileSystem.get(conf);

    job.setNumReduceTasks(8);
    job.setJarByClass(ChiSquarePrepro.class);
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
