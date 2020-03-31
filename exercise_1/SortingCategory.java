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

//Input: term@category  chisquare_value
public class SortingCategory {

  public static class TokenizerMapper
       extends Mapper<Object, Text, IntWritable, Text>{

    private final static IntWritable ONE = new IntWritable(1);
    private Text word = new Text();
    private final JSONParser parser = new JSONParser();
    

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

      
      /*HashMap<String, Integer> allCategories = new HashMap<String, Integer>();
      Configuration config = context.getConfiguration();
      String categoryCounts = config.get("categoryCounts");
      for (String val : categoryCounts.split(",")){
        String [] cat_count = val.split("\\s+");
        allCategories.put(cat_count[0], Integer.parseInt(cat_count[1]));
      }
      context.write(word, ONE);
    } */

    String[] valueParsed = value.toString().split("\\s+");
    Integer chiSq = Integer.parseInt(valueParsed[1]); 
    String val = valueParsed[0];
    
    context.write(new IntWritable(chiSq), new Text(val));
    
    }
  }


  //Partitioner class
	
   public static class ChiSquarePartitioner extends
   Partitioner < IntWritable, Text >
   {
      @Override
      public int getPartition(IntWritable key, Text value, int numReduceTasks)
      {
         String category = value.toString().split("@")[1];
         
         if(numReduceTasks == 0)
         {
            return 0;
         }
         
         if(category.equals("Automotive") || category.equals("Book"))
         {
            return 0;
         }
         else if(category.equals("CDs_and_Vinyl") || category.equals("Health_and_Personal_Care"))
         {
            return 1 % numReduceTasks;
         }
         else if(category.equals("Kindle_Store") || category.equals("Apps_for_Android"))
         {
            return 2 % numReduceTasks;
         }
         else if(category.equals("Baby") || category.equals("Beauty"))
         {
            return 3 % numReduceTasks;
         }
         else if(category.equals("Cell_Phones_and_Accessorie") || category.equals("Clothing_Shoes_and_Jewelry"))
         {
            return 4 % numReduceTasks;
         }
         else if(category.equals("Digital_Music") || category.equals("Electronic"))
         {
            return 5 % numReduceTasks;
         }
         else if(category.equals("Grocery_and_Gourmet_Food") || category.equals("Home_and_Kitche"))
         {
            return 6 % numReduceTasks;
         }
         else if(category.equals("Movies_and_TV") || category.equals("Musical_Instrument"))
         {
            return 7 % numReduceTasks;
         }
         else if(category.equals("Office_Product") || category.equals("Patio_Lawn_and_Garde"))
         {
            return 8 % numReduceTasks;
         }
         else if(category.equals("Pet_Supplie") || category.equals("Sports_and_Outdoor"))
         {
            return 9 % numReduceTasks;
         }
         else if(category.equals("Tools_and_Home_Improvement") || category.equals("Toys_and_Game"))
         {
            return 10 % numReduceTasks;
         }
         return 0;
      }
   }


   public static class MyKeyComparator extends WritableComparator {
      public MyKeyComparator() {
          super(IntWritable.class, true);
      }

      @SuppressWarnings("rawtypes")
      @Override
      public int compare(WritableComparable w1, WritableComparable w2) {
          IntWritable key1 = (IntWritable) w1;
          IntWritable key2 = (IntWritable) w2;          
          return -1 * key1.compareTo(key2);
      }
  } 

  public static class IntSumReducer
       extends Reducer<IntWritable,Text,Text,Text> {

    public void reduce(IntWritable key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      String category = "";
      String term = "";
      for (Text val : values) {
        String[] termCat = val.toString().split("@");
        if (termCat.length == 2){
          category = val.toString().split("@")[1];
          term = val.toString().split("@")[0];
          context.write(new Text(key.toString() + "@" + category), new Text(term));
          //context.write(new Text("NOPE_In_IF"), val);
        }
        else{
          context.write(new Text("NOPE"), val);
        }
      }
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

    Job job = Job.getInstance(conf, "SortingCategory");
    job.setNumReduceTasks(11);
    job.setPartitionerClass(ChiSquarePartitioner.class);
    job.setSortComparatorClass(MyKeyComparator.class);
    job.setJarByClass(SortingCategory.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    //List<String> swords = new ArrayList<String>();
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
