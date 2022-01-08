import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class OldTreeMapper extends Mapper<LongWritable, Text, NullWritable, Age_Arrond_Writable>
{

    private NullWritable cleI = NullWritable.get();
    private Age_Arrond_Writable valeurI = new Age_Arrond_Writable();

    @Override
    public void map(LongWritable cleE, Text valeurE, Context context) throws IOException, InterruptedException
    {
        
        if (cleE.get() == 0L) return;
        
        String line = valeurE.toString();
        int a1, a2;

        try {

            Arbre.ExtractLine(line);
            a1 = Integer.parseInt(Arbre.getARRONDISSEMENT());
            a2 = Integer.parseInt(Arbre.getANNEE());
            valeurI.set(a1,a2);
            context.write(cleI, valeurI);

        } catch (Exception e) {
            System.err.println(e.getStackTrace()[0]+": "+e.getMessage()+" => "+e+" sur la ligne "+line);
        }
    }
}
