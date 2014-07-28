package joiner.computational;

public class Domain {
	
	// set the domain of the data
		private float min ;
		private float max ;
		private float domainSize ;

		public Domain()
		{
			this.min = 0;
			this.max = 10;
			this.domainSize = max - min ;			
		}
		
		public float getMin()
		{	return this.min ;	}
		
		public float getMax()
		{	return this.max ; 	}
		
		public float getDomainSize()
		{	return	this.domainSize ;	}
}
