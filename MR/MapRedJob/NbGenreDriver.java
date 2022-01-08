import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class NbGenreDriver extends Configured implements Tool
{

    @Override
    public int run(String[] args) throws Exception
    {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf, "NbGenre Job");
        job.setJarByClass(NbGenreDriver.class);
        job.setMapperClass(NbGenreMapper.class);
        job.setReducerClass(NbGenreReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));   
        FileInputFormat.setInputDirRecursive(job, false);     
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1])); 

    }
    

    public static void main(String[] args) throws Exception {
        NbGenreDriver driver = new NbGenreDriver();
        int exitCode = ToolRunner.run(driver, args);
        System.exit(exitCode);
      }
}
