import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Age_Arrond_Writable implements Writable {

        private int annee;
        private int arrondissement;

        public Age_Arrond_Writable(){
                annee = 1000000;
                arrondissement = -1;
              }
       
        public void write(DataOutput sortie) throws IOException {
                sortie.writeInt(annee);
                sortie.writeInt(arrondissement);
           }
        public void readFields(DataInput entree) throws IOException {
                annee = entree.readInt();
                arrondissement = entree.readInt();
           }
        
        public void set(int v1, int v2) {
                arrondissement = v1;
                annee = v2;
           }
        
        public  void merge(Age_Arrond_Writable v){
                if(v.annee <= annee){
                       annee = v.annee;
                       arrondissement = v.arrondissement;
                     } 
                 }
        
        public String toString() {
            return "Arrondissement voulu est: " + arrondissement + "  L'age est: " + annee;
          }

}