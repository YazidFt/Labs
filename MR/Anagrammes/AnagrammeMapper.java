import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.*;



public class AnagrammeMapper extends Mapper<LongWritable, Text, Text, Text> {

     private Text k = new Text();
     private Text v = new Text();

     @Override
     public void map(LongWritable idx, Text val, Context context) throws IOException, InterruptedException {
             v = val;
             String s = val.toString();
             char c[] = s.toCharArray();
             Arrays.sort(c);
             k.set(new String(c));
             context.write(k, v);
         }

 }