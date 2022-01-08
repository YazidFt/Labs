import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class NbGenreMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{

    private Text cleI = new Text();
    private IntWritable valeurI = new IntWritable();

    @Override
    public void map(LongWritable cleE, Text valeurE, Context context)
            throws IOException, InterruptedException
    {

        if (cleE.get() == 0L) return;

        String line = valeurE.toString();

        try {

            Arbre.ExtractLine(line);
            cleI.set(Arbre.getGenre());
            valeurI.set(1);
            context.write(cleI, valeurI);

        } catch (Exception e) {
            System.err.println(e.getStackTrace()[0]+": "+e.getMessage()+" => "+e+" sur la ligne "+line);
        }
    }
}
