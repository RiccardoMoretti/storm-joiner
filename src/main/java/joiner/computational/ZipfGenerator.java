package joiner.computational;

import java.util.Random;

/* Zipf disribution implements Zipf-like law. According to Zipf's law the
  		most frequent object has probability twice the second most frequent,
  		three times the third most frequent object, etc. 
  		
  	More info at http://en.wikipedia.org/wiki/Zipf's_law.						*/
 
public class ZipfGenerator {
	
     private Random rnd = new Random(System.currentTimeMillis());
    
     private int size;
     
/*   Skew varies the probability "step" between oids. High skew would give
     	more preference to the recent oids. Low skew (< 0.3) would give
     	preference to older oids. */
     private double skew;
     
     private double bottom = 0;

     public ZipfGenerator(int size, double skew) {
             
    	 	this.size = size;
            this.skew = skew;

            for (int i = 1; i < size; i++) 
                     this.bottom += (1 / Math.pow(i, this.skew));
            
     }
  
     public int nextInt() {
    	 
             int rank;
             double friquency = 0;
             double dice;

             rank = rnd.nextInt(size);
             friquency = (1.0d / Math.pow(rank, this.skew)) / this.bottom;
             dice = rnd.nextDouble();

             while (!(dice < friquency)) {
                     rank = rnd.nextInt(size);
                     friquency = (1.0d / Math.pow(rank, this.skew)) / this.bottom;
                     dice = rnd.nextDouble();
             }

             return rank;
     }

}

