import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class VarianceMapper extends Mapper<LongWritable, Text, NullWritable, VarianceWritable>{

        private NullWritable k = NullWritable.get();
        private VarianceWritable v = new VarianceWritable();

        @Override
        public void map(LongWritable keyE,Text valE, Context context)
                   throws IOException, InterruptedException{

            String line = valE.toString();

          try{    
              v.set((double)line.length());
              context.write(k,v);

           }catch (Exception e) {
               //Not Ok
            }
           
          }    
  }

    