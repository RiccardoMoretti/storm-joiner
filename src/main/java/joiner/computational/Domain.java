package joiner.computational;

public class Domain {
	
	// set the domain of the data
		private float min ;
		private float max ;
		private float domainSize ;
		private float soglia ;
		private float disc[] ;
		private int n;

		public Domain()
		{
			this.min = 0;
			this.max = 10;
			this.domainSize = max - min ;
			soglia = 1 ;
			
			CreateRange();
		}
		
/*	Permette di suddividere il dominio in intervalli grandi " 2 * soglia ", 
 * 		ogni etichetta dell'intervallo 	sarà il valore discreto 
 * 		che verrà associato successivamente ad ogni istanza.
*/
			
			public void CreateRange( )
			{	
				float inc = 2 * soglia ;
				int j = 0 ;
				
				// Inizializzazione numero totale degli intervalli, che corrisponde al numero degli elementi discreti
				// a cui andrò poi ad associare ogni istanza
				if ( (( max - min ) % inc ) == 0 )
					n = ( int ) (( max - min ) / inc ) + 1 ;
				else
					n = ( int ) (( max - min ) / inc ) + 2 ;
				
				disc = new float [ n ] ;						// Creo array numero discreti
				
				for ( float i = min ; i < max ; i = i + inc )
				{
					disc [ j ] = i ;
					j++ ; 					
				}
				
				if ( ( disc[n-1] ) == 0 )
					disc [ j ] = max ;
		
		/*
				System.out.println ( " INTERVALLI / NUMERI DISCRETI " ) ;
				System.out.println ( " Numero totale : " + n ) ;
				for ( int q = 0 ; q < n ; q ++ )
					System.out.println ( " ETICHETTA " + disc[q] ) ;	*/
			
			}
			
		public float[] getDisc()
		{ 	return this.disc; 		}
		
		public int getN()
		{	return this.n ;	}
		
		public float getMin()
		{	return this.min ;	}
		
		public float getMax()
		{	return this.max ; 	}
		
		public float getDomainSize()
		{	return	this.domainSize ;	}
}
