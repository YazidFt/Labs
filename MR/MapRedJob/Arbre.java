

public abstract class Arbre {

        static String[] features;

        public static void ExtractLine(String line){
              features = line.split(";");
           }
       
        public static String getARRONDISSEMENT(){
              return features[1];
           } 
        
        public static String getGenre(){
              return features[2];
           }
}