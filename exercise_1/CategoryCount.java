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

public class CategoryCount {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable ONE = new IntWritable(1);
    private Text word = new Text();
    private final JSONParser parser = new JSONParser();
    

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

      JSONObject jsonObj;
      try {
      jsonObj = (JSONObject) parser.parse(value.toString());
      }
      catch (ParseException e){
        e.printStackTrace();
        return;
      }
      String category = jsonObj.get("category").toString();
      //StringTokenizer itr = new StringTokenizer(review, " .!?,;:()[]{}-_\"'~#&*%$");
      category = category.replace("\"", "");
      word.set(category.trim());
      context.write(word, ONE);
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      context.write(key, new IntWritable(sum));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "parsing");
    job.setNumReduceTasks(2);
    job.setJarByClass(CategoryCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    //List<String> swords = new ArrayList<String>();
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
