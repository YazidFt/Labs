import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;



public class NbGenreReducer extends Reducer<Text, IntWritable, Text, IntWritable>
{
    
    private Text cleS;
    private IntWritable valeurS = new IntWritable();

    @Override
    public void reduce(Text cleI, Iterable<IntWritable> valeursI, Context context)
            throws IOException, InterruptedException
    {
        
        cleS = cleI;

        int resultat = 0;
        for (IntWritable valeurI : valeursI) {
            int val = valeurI.get();
            resultat += val;
          }
        valeurS.set(resultat);

        context.write(cleS, valeurS);
    }

}
