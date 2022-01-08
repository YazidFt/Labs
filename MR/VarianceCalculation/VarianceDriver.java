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



public class VarianceDriver extends Configured implements Tool{

    @Override
    public int run(String[] args) throws Exception{
        //Configuration
        Configuration conf =  this.getConf();
        Job job = Job.getInstance(conf, "Variance");

        job.setJarByClass(VarianceDriver.class);
        job.setMapperClass(VarianceMapper.class);
        job.setCombinerClass(VarianceCombiner.class);
        job.setReducerClass(VarianceReducer.class);

        //Input
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.setInputDirRecursive(job, false);     

        //Output
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(VarianceWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1])); 

        //End
         boolean success = job.waitForCompletion(true);
         return success ? 0 : 1;

    }

    public static void main(String[] args) throws Exception{
         VarianceDriver driver = new VarianceDriver();
         int exitCode = ToolRunner.run(driver, args);
         System.exit(exitCode);
      }

}