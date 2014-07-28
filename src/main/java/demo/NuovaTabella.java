
public class NuovaTabella {
	
/* 	Ogni istanza è composta da un'array di float  " att " che sono le istanze degli attributi
 * 	L.I / R.I della tabella di partenza da joinare e dal/ dai corrispondente / corrispondenti 
 *  valore / valori discreto / discreti associato / associati ( uno solo per la tabella L,
 *  due per la tabella R ).	 */
	
	float att[], jatt[] ;
	
	public NuovaTabella ( int dim )			
	{
		att = new float [dim] ;
		jatt = new float [dim] ;
	}

/*	Meotodo costruttore che,  in base ai parametri d'ingresso, crea una nuova istanza composta 
 * 	da " dim " tuple ( se L ) o da " 2 * dim " tuple ( se R ) e inizilizza i valori basandosi 
 *  sull'array " arr " che riceve in input ( composto da tutte le istanze L.I/R.I della tabella 
 *  di partenza da joinare ).	*/
	
	public NuovaTabella ( int dim , float[] arr , float mas, char lr )
	{	
		if ( lr == 'L')								// Caso tabella L
			{	att = new float [dim] ;
				jatt = new float [dim] ;
		
				for ( int i = 0 ; i < dim ; i++ )
					{
						att[i] = arr[i] ;
						jatt[i] = mas ;
					}
			}
		else										// Caso tabella R
		{
			att = new float [2*dim] ;
			jatt = new float [2*dim] ;
			int cont = 0 ;
			
			for ( int i = 0 ; i < dim ; i++ )
			{
				att[cont] = arr[i] ;
				jatt[cont] = mas ;
				att[cont+1] = arr[i] ;
				jatt[cont+1] = mas ;
				
				cont++ ;
			}
		}			
	}

/* 	Setta l'attributo in posizione " pos " al valore " a "		*/
	
	public void setAtt ( int pos, float a )
	{
		att[pos] = a ;
	}

/*	Restituisce l'attributo in posizione " pos " 	*/
	
	public float getAtt ( int pos )
	{
		return att [ pos ] ;
	}
	
/* 	Setta l'attributo discreto ( su cui si basa l'equi-join ) in posizione " pos " al valore " ja "	*/
	
	public void setJAtt ( int pos, float ja )
	{
		jatt[pos] = ja ;
	}

/*	Restituisce l'attributo discreto ( su cui si basa l'equi-join ) in posizione " pos " 	*/
	
	public float getJAtt ( int pos )
	{
		return jatt [ pos ] ;
	}		
}
