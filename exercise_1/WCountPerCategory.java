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

public class WCountPerCategory {

  public static ArrayList<String> stopwords = new ArrayList<String>();

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
      category = category.replace("\"", "");

      String review = jsonObj.get("reviewText").toString();
      StringTokenizer itr = new StringTokenizer(review, " .!?,;:<>()[]{}-_\"'~#&*%$");
      while (itr.hasMoreTokens()) {
        String term = itr.nextToken().toLowerCase();
        if (term.length() > 1){
          word.set(term + "@" + category);
          context.write(word, ONE);
          }
      }
    }
  }

  /*public static class IntSumCombiner
       extends Reducer<Text,IntWritable,Text,Text> {

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }

      //splitting the key:
      String[] keyParts = key.toString().split("@");
      
      if (keyParts[0].length() > 0){
      context.write(new Text(keyParts[0]), new Text(keyParts[1] + ":" + Integer.toString(sum)));
      }
    }
  }*/ 

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      
      //HashMap<String, String> categoryCounts = new HashMap<String, String>();
      /*for (Text t : values){
        String[] valueParts = t.toString().split(":");
        categoryCounts.put(valueParts[0], valueParts[1]);
      }

      context.write(key, new Text(categoryCounts.toString())); */
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      context.write(key, new IntWritable(sum));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "WCountPerCategory");
    job.setNumReduceTasks(2);
    job.setJarByClass(WCountPerCategory.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    //List<String> swords = new ArrayList<String>();
    try {
			Scanner scanner = new Scanner(new File(args[2]));
			while (scanner.hasNextLine()) {
				stopwords.add(scanner.nextLine().trim());
			}
			scanner.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
    //split("\\s+");

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
