import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class AnagrammeDriver extends Configured implements Tool {

  
          @Override
          public int run(String[] args) throws Exception{
               
               Configuration conf =  this.getConf();
               Job job = Job.getInstance(conf, "Anagramme");

               job.setJarByClass(AnagrammeDriver.class);
               job.setMapperClass(AnagrammeMapper.class);
               job.setReducerClass(AnagrammeReducer.class);

               job.setInputFormatClass(TextInputFormat.class);
               FileInputFormat.addInputPath(job, new Path(args[0]));
               FileInputFormat.setInputDirRecursive(job, false); 

               job.setMapOutputKeyClass(Text.class);
               job.setMapOutputValueClass(Text.class);
               job.setOutputKeyClass(NullWritable.class);
               job.setOutputValueClass(Text.class);
               job.setOutputFormatClass(TextOutputFormat.class);
               FileOutputFormat.setOutputPath(job, new Path(args[1])); 


               //End
               boolean success = job.waitForCompletion(true);
               return success ? 0 : 1;
            }

          public static void main(String[] args) throws Exception{
                  AnagrammeDriver driver = new AnagrammeDriver();
                  int exitCode = ToolRunner.run(driver, args);
                  System.exit(exitCode);
               }
     
    }