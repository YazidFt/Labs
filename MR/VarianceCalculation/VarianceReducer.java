import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class VarianceReducer extends Reducer<NullWritable, VarianceWritable, NullWritable, DoubleWritable>{

        private DoubleWritable vs = new DoubleWritable();
        private VarianceWritable v_w = new VarianceWritable();

    
        @Override
        public void reduce(NullWritable keyE, Iterable<VarianceWritable> valE, Context context)
                   throws IOException, InterruptedException{

            v_w.clear();

            for(VarianceWritable v : valE){
                  v_w.add(v);
              }
            
            vs.set(v_w.getVariance());
            context.write(keyE, vs);
                  
          }    
  }

    