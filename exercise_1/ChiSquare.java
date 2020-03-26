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

public class ChiSquare {

  public static ArrayList<String> stopwords = new ArrayList<String>();
  public static HashMap<String, Integer> categoryCounts = new HashMap<String, Integer>();
 
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      
      String[] raw = value.toString().split("\\s+");
      String[] termCat = raw[0].split("@");
      //Integer count = Integer.parseInt(raw[1].trim());
      context.write(new Text(termCat[0].trim()), new Text(termCat[1] + ":" + raw[1]));
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,IntWritable> {

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      
      
      HashMap<String, Integer> termCounts = new HashMap<String, Integer>();
      for (Text t : values){
        String[] valueParts = t.toString().split(":");
        termCounts.put(valueParts[0].trim(), Integer.parseInt(valueParts[1]));
      }


      //Calculating Chi Square:
      int a = 0;
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
        context.write(new Text(key.toString() + "@" + keyG), new IntWritable(chisq));
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "ChiSquare");
    job.setNumReduceTasks(2);
    job.setJarByClass(ChiSquare.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    try {
			Scanner scanner = new Scanner(new File(args[2]));
			while (scanner.hasNextLine()) {
				stopwords.add(scanner.nextLine().trim());
			}
			scanner.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

     try {
			Scanner scanner_categories = new Scanner(new File(args[3]));
			while (scanner_categories.hasNextLine()) {
        String [] categoryC = scanner_categories.nextLine().split("\\s+");
				categoryCounts.put(categoryC[0].trim(), Integer.parseInt(categoryC[1]));
			}

			scanner_categories.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
    System.out.println("categories BITCH -------------------------------------------------------------------------");
    System.out.println(categoryCounts);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
