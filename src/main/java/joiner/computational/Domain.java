package joiner.computational;

public class Domain {
	
	// set the domain of the data
		private float min ;
		private float max ;
		private float domainSize ;
		private float threshold ;
		
		private float disc[] ;
		private int n;
	
		
		public Domain(float min, float max, float threshold)
		{
			this.min = min;
			this.max = max;
			this.domainSize = max - min ;
			this.threshold = threshold ;
			
			CreateRange();
		}
		
		
		//Split the domain in range with size = 2*threshold.
		public void CreateRange( )
			{	
				float inc = 2 * threshold;
				int j = 0 ;
				
				if ( (( max - min ) % inc ) == 0 )
					n = ( int ) (( max - min ) / inc ) + 1 ;
				else
					n = ( int ) (( max - min ) / inc ) + 2 ;
				
				//array of range's label
				disc = new float [ n ] ;						
				
				for ( float i = min ; i < max ; i = i + inc )
				{
					disc [ j ] = i ;
					j++ ; 					
				}
				
				if ( ( disc[n-1] ) == 0 )
					disc [ j ] = max ;
		
				//System.out.println ( " NUMBERS OF RANGE : " + n ) ;	 																					
			
			}
			
			
		public float[] getDisc()
		{ 	return this.disc; 			}
		
		public int getN()
		{	return this.n ;				}
		
		public float getThreshold()
		{	return this.threshold ;		}
		
		public float getMin()
		{	return this.min ;			}
		
		public float getMax()
		{	return this.max ; 			}
		
		public float getDomainSize()
		{	return	this.domainSize ;	}

}
