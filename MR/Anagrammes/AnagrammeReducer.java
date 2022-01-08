import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.*;


public class AnagrammeReducer extends Reducer<Text, Text, NullWritable, Text> {

      private NullWritable k = NullWritable.get();
      private Text v = new Text();

      
      public void reduce(NullWritable kE, Iterable<Text> vE, Context context)
               throws IOException, InterruptedException{
             
              String res = "";
              int cnt = 0;

              for(Text t : vE){
                    res +=   (" | "+ t);
                    cnt++;
                }
              
              if(cnt > 1){
                 v.set(res);
                 context.write(k, v);
                }
                  
            }   
       
    }

 
 