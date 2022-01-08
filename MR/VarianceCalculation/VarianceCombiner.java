import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class VarianceCombiner extends Reducer<NullWritable, VarianceWritable, NullWritable, VarianceWritable>{

        private NullWritable ks = NullWritable.get();
        private VarianceWritable vs = new VarianceWritable();

        @Override
        public void reduce(NullWritable keyE, Iterable<VarianceWritable> valE, Context context)
                   throws IOException, InterruptedException{

          //IMPORTANT
          vs.clear();
     
          for(VarianceWritable v : valE){
                  vs.add(v);    
            }

           context.write(ks, vs);
              
         }    
  }

    