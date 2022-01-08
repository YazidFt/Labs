import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



public class OldTreeReducer extends Reducer<NullWritable, Age_Arrond_Writable, NullWritable, Text>
{
    
    private Text valeurS = new Text();

    @Override
    public void reduce(NullWritable cleI, Iterable<Age_Arrond_Writable> valeursI, Context context)
            throws IOException, InterruptedException
    {
        
        Age_Arrond_Writable result = new Age_Arrond_Writable();
        for (Age_Arrond_Writable valeurI : valeursI) {
              result.merge(valeurI);
           }
        
        valeurS.set(result.toString());

        context.write(cleI, valeurS);
    }

}
