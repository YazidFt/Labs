import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class OldTreeDriver extends Configured implements Tool
{

    @Override
    public int run(String[] args) throws Exception
    {
        //Current Time
        Long initTime = System.currentTimeMillis();

        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "Old Tree Job");
        job.setJarByClass(OldTreeDriver.class);
        job.setMapperClass(OldTreeMapper.class);
        job.setReducerClass(OldTreeReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));   
        FileInputFormat.setInputDirRecursive(job, false);     
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Age_Arrond_Writable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1])); 


        Long startTime = System.currentTimeMillis();
        boolean success = job.waitForCompletion(true);
        Long endTime = System.currentTimeMillis();
        System.out.println("Job Duration seconds: " + ((endTime-startTime)/1000L));
        System.out.println("Total Duration seconds: " + ((endTime-initTime)/1000L));
        return success ? 0 : 1;
    }
    

    public static void main(String[] args) throws Exception {
        OldTreeDriver driver = new OldTreeDriver();
        int exitCode = ToolRunner.run(driver, args);
        System.exit(exitCode);
      }
}
