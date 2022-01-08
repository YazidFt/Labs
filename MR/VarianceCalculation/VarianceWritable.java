import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


public class VarianceWritable implements Writable{

          private Long nb;
          private Double Sx;
          private Double Sx2;


          public void VarianceWritable(){
                  clear();
               }

         public void clear(){
                nb = 0L;
                Sx = 0.0;
                Sx2 = 0.0; 
              }
                  
          
          public void write(DataOutput sortie) throws IOException {
                  sortie.writeLong(nb);
                  sortie.writeDouble(Sx);
                  sortie.writeDouble(Sx2);
               }

          public void readFields(DataInput entree) throws IOException {
                  nb = entree.readLong();
                  Sx = entree.readDouble();
                  Sx2 = entree.readDouble();
              }
          
          
          public void set(Double x){
                  nb = 1L;
                  Sx = x;
                  Sx2 = x*x; 
              }
          
          public void add(VarianceWritable vw){
                  this.nb += vw.nb;
                  this.Sx += vw.Sx; 
                  this.Sx2 += vw.Sx2;
             }

         
          public double getVariance(){
                  double mx = Sx/nb;
                  double mx2 = Sx2/nb;
                  return mx2 - mx*mx;
            }

          public String toString(){
                  return "VarianceWritable(n="+nb+", Sx="+Sx+", Sx 2 ="+Sx2+")";
            }


      }