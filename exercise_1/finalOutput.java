import java.io.IOException;
import java.util.StringTokenizer;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import java.util.*; 
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;
import java.util.Map.*;

public class finalOutput {
  /***********************************************************
  Since we only want the top 200 entries for each category, the output of SortingCategory.java (<(chisquare@category), term>).
  There are 22 output files (one for each category).
  i used the hdfs command on each of the files to read the top 200 entries and write them to a local file:

  hadoop fs -cat hdfs:///user/e1267533/exercise_1/SortingCategory/part-r-00000 | head -200 >  cat0.txt

  Followed by 

  hadoop fs -put cat0.txt hdfs:///user/e1267533/exercise_1/SortingCategoryShort/cat0.txt

  to write it back to the hadoop filesystem.
  I did this for every one of the 22 files.

  This MapReduce job reads all the files and writes them into the desired output format: <category> term:chisquare term:chisquare....

  The last line (dictionary of all the term appearning here) is then created in a separate python file by me, i believe that doing that with a 
  mapreduce job would have been overkill.

  The output.txt file of this job is processed like this to create the last line (dictionary):

  f = open("output.txt", "r")
  all_terms = list()
  for x in f:
      terms = x.split()
      ignore_first = True
      for t in terms:
          if not ignore_first:
              all_terms.append(t.split(":")[0])
          else:
              ignore_first = False

  all_terms.sort()

  for t in all_terms:
    line += t + " "
  
  outF = open("myOutFile.txt", "w")
  outF.write(line)
  ************************************************************/

  //maps the input <(chisquare@category), term> to <category, (term:chisquare)>
  public static class TokenizerMapper
       extends Mapper<LongWritable, Text, Text, Text>{

    
    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {

    //input: 0.0021270579@Automotive	wheel
    
    String[] valueParsed = value.toString().split("\\s+");
    String[] valueParsedSplit = valueParsed[0].split("@");
    String chiSq = valueParsedSplit[0]; 
    String category =valueParsedSplit[1];
    String term = valueParsed[1];
    context.write(new Text(category), new Text(term + ":" + chiSq));
         
    
    }
  }


  //reducer gets the <category, (term:chisquare)> tuples and writes the term and their 
  //respective chisquare value for the category into a hashmap.
  //In order to sort the values in the hashmap in descending order without loosing duplicates
  //Collections.sort is used on the hashmap.
  public static class IntSumReducer
       extends Reducer<Text,Text,Text,NullWritable> {

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      String category = "";
      String term = "";

      //creating the map containing the term as key and the chisquare value as value.
      HashMap<String,Double> map = new HashMap<String,Double>();
      String[] tmp;
      for (Text val : values) {
          tmp = val.toString().split(":");
        map.put(tmp[0], Double.parseDouble(tmp[1]));
      }



    //creating a linkedlist to sort the entries from map
    List<Entry<String, Double>> list = new LinkedList<Map.Entry<String, Double>>(map.entrySet());

    //sorting the list with a comparator
    Collections.sort(list, new Comparator<Entry<String, Double>>() {
        public int compare(Map.Entry<String, Double> o1, Map.Entry<String, Double> o2) {
            return (-1)*(o1.getValue()).compareTo(o2.getValue());
        }
    });

    //convert LinkedList back to Map
    Map<String, Double> sortedMap = new LinkedHashMap<String, Double>();
    for (Entry<String, Double> entry : list) {
        sortedMap.put(entry.getKey(), entry.getValue());
    }
      //create a string with the sorted ChiSquare values and terms
      StringBuilder str = new StringBuilder();
      str.append("<" + key.toString() +">" + " ");
      for(Map.Entry m:sortedMap.entrySet()){    
        str.append(" " + m.getKey()+":"+m.getValue());    
      }
      //write it to file.
      context.write(new Text(str.toString()), NullWritable.get());    
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf, "SortingCategory");
    job.setNumReduceTasks(8);
    job.setJarByClass(finalOutput.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    //List<String> swords = new ArrayList<String>();
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
