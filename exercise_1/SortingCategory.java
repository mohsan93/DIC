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
import org.apache.hadoop.io.DoubleWritable;
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

public class SortingCategory {
  /***************************************
    Now  that the ChiSquare values for each term per catgory have been calculated (input: <(term$$category),chisquare>),
    I wanted to sort the terms for each category by its chiSquare value and write it to a different file for each category.
    For the sorting a custom comparator (MyKeyComparator) is implemented that returns the sorted keys in descending order.
    To create an output file for each category (containing the sorted terms in descending order) i decided to use a partitioner.
    The mapper outputs: <chisquare, (term$$category)>, this is then given to the partitioner. The partitioner takes a 
    look at the category in the value-part of the key-value pair and decides then to which reducer the tuple is sent to.
    Since there are 22 categories, there are 22 reducers used (not the most efficient way), the reducers then sort their entries
    in descending order of the key (chisquare value) and output it to a file each.
   **************************************/
  
  //mapper, the key-output in this case is DoubleWriteable because of the ChiSquare Value
  public static class TokenizerMapper
       extends Mapper<Object, Text, DoubleWritable, Text>{

    private Text word = new Text();
    private final JSONParser parser = new JSONParser();
    

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {


    String[] valueParsed = value.toString().split("\\s+");
    Double chiSq = Double.parseDouble(valueParsed[1]); 
    String val = valueParsed[0];
    
    context.write(new DoubleWritable(chiSq), new Text(val));
    
    }
  }


    //Partitioner class
    //The Partitioner takes a look at the category in the value part of the pair and then
    //decided to which reducer to send the tuple to.
    //This is done via a switch-case statement.
    //It returns an integer, ranging from 0 to 21, so there are a total of 22 reducers,
    //one for each category.
   public static class ChiSquarePartitioner extends
   Partitioner < DoubleWritable, Text >
   {
      @Override
      public int getPartition(DoubleWritable key, Text value, int numReduceTasks)
      {
         String category = value.toString().split("@")[1];
         
         if(numReduceTasks == 0)
         {
            return 0;
         }

         switch(category) 
        { 
            case "Automotive": 
                return 0;
               
            case "Book": 
                return 1;
                
            case "CDs_and_Vinyl": 
                return 2;
               
            case "Health_and_Personal_Care": 
                return 3;
               
            case "Kindle_Store": 
                return 4;
                
            case "Apps_for_Android": 
                return 5;
               
            case "Baby": 
                return 6;
               
            case "Beauty": 
                return 7; 
                
            case "Cell_Phones_and_Accessorie": 
                return 8;
               
            case "Clothing_Shoes_and_Jewelry": 
                return 9;
               
            case "Digital_Music": 
                return 10;
                
            case "Electronic": 
                return 11;
               
            case "Grocery_and_Gourmet_Food": 
                return 12;
               
            case "Home_and_Kitche": 
                return 13;
                
            case "Movies_and_TV": 
                return 14;
               
            case "Musical_Instrument": 
                return 15;
               
            case "Office_Product": 
                return 16;
                
            case "Patio_Lawn_and_Garde": 
                return 17;
               
            case "Pet_Supplie": 
                return 18;
               
            case "Sports_and_Outdoor": 
                return 19;
                
            case "Tools_and_Home_Improvement": 
                return 20;
               
            case "Toys_and_Game": 
                return 21;
               
            default: 
                return 0; 
        } 
      }
   }

    //Custom Comparator, returns the sorted keys in descending order
   public static class MyKeyComparator extends WritableComparator {
      public MyKeyComparator() {
          super(DoubleWritable.class, true);
      }

      @SuppressWarnings("rawtypes")
      @Override
      public int compare(WritableComparable w1, WritableComparable w2) {
          DoubleWritable key1 = (DoubleWritable) w1;
          DoubleWritable key2 = (DoubleWritable) w2;          
          return -1 * key1.compareTo(key2);
      }
  } 

 //There is a reducer for each category.
 //The reducer accepts the key-value pair <chisquare, (term$$category)>, and outputs 
 //<(chisquare@category), term> via simple string manipulation.
 //output is sorted by chisquare value.

  public static class IntSumReducer
       extends Reducer<DoubleWritable,Text,Text,Text> {

    public void reduce(DoubleWritable key, Iterable<Text> values,
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
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    /*String categoryCounts = "";
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
    conf.set("categoryCounts", categoryCounts); */ 

    Job job = Job.getInstance(conf, "SortingCategory");
    //number of reducer tasks is set to 22, one for each category.
    job.setNumReduceTasks(22);
    job.setPartitionerClass(ChiSquarePartitioner.class);
    job.setSortComparatorClass(MyKeyComparator.class);
    job.setJarByClass(SortingCategory.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setMapOutputKeyClass(DoubleWritable.class);
    job.setMapOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    //List<String> swords = new ArrayList<String>();
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
