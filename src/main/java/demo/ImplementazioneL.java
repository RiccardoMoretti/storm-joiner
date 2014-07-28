
public class ImplementazioneL {

/* 	Dichiarazione variabili utili per implementare l'algoritmo proposto.	*/
	
	Read r = new Read () ;							//Utile per gestire l'I/O
	float L[], disc[], vmin, vmax, soglia ;
	int dim , n ;							
	NuovaTabella l;								
	
/* 	Metodo costruttore che inizializza l'oggetto con i valori del dominio e di soglia passati 
 * 	come parametri d'ingresso.
 * 	Inoltre permette di creare la tabella, ricevendo in input il numero di tuple e i
 * 	valori delle varie istanze di L.I.		*/
	
	public ImplementazioneL( float min, float max, float s )
	{	    
		// Creazione tabella di partenza
		 	System.out.println( "Inserire il numero di tuple della tabella : " ) ;
		 	dim = r.readInt( ) ;
		 	
		 	L = new float[ dim ] ;	
		 	
		// Inserimento varie istanze ( tuple della tabella )
			for ( int u = 0 ; u < dim ; u++ )
			{
				System.out.println( "Inserire l'elemento numero " + ( u + 1 ) + " della tabella : " ) ;
				L[ u ] = r.readFloat( ) ;
			}
		
		// Inizializzazione dominio e soglia
			vmin = min ;
			vmax = max ;
			soglia = s ;
			
		// Creazione oggetto utile in seguito	
			l = new NuovaTabella ( dim , L, vmax, 'L' ) ;
	}

/*	Permette di suddividere il dominio in intervalli grandi " val * soglia ", ogni etichetta dell'intervallo
 * 	sarà il valore discreto che verrà associato successivamente ad ogni istanza.
 * 	Perchè il metodo funzioni " val " deve essere >= 2 ( do per scontato inpunt corretto ). 	 */
	
	public void creaIntervalli( int val )
	{	
		float inc = val * soglia ;
		int j = 0 ;
		
		// Inizializzazione numero totale degli intervalli, che corrisponde al numero degli elementi discreti
		// a cui andrò poi ad associare ogni istanza
		if ( (( vmax - vmin ) % inc ) == 0 )
			n = ( int ) (( vmax - vmin ) / inc ) + 1 ;
		else
			n = ( int ) (( vmax - vmin ) / inc ) + 2 ;
		
		disc = new float [ n ] ;						// Creo array numero discreti
		
		for ( float i = vmin ; i < vmax ; i = i + inc )
		{
			disc [ j ] = i ;
			j++ ; 					
		}
		
		if ( ( disc[n-1] ) == 0 )
			disc [ j ] = vmax ;
		
		System.out.println ( " INTERVALLI / NUMERI DISCRETI " ) ;
		System.out.println ( " Numero totale : " + n ) ;
		for ( int q = 0 ; q < n ; q ++ )
			System.out.println ( " ETICHETTA " + disc[q] ) ;	
	}
	
/* 	Meotodo chiave che implementa il concetto fondamentale dell'algoritmo proposto.
 * 	Ogni istanza di L.I viene associata al valore discreto ( etichetta intervallo ) più vicino.
 * 	Questa associazione introduce un nuovo attributo JAtt che sarà quello su cui si baserà l'equi join.
 * 	Il numero di tuple della nuova tabella creata è uguale a quello della tabella di partenza.	 */
	
	public void discretizza ()
	{
			float temp = vmax;
			int index = 0 ;
			
		for ( int i = 0 ; i < dim ; i++ )
		{
			index = 0 ;
			temp = vmax ;
			
			for ( int j = 0 ; j < n ; j++ )
			{
					if ( Math.abs( L[i] - disc[j] ) < temp )
					{
						temp = Math.abs( L[i] - disc[j] );
						index = j ;
					}
			}
			
			l.setJAtt( i, disc[index] ) ;	
			
		}		
	}
	
/*	Ritorna il numero di tuple della tabella di partenza ( uguale a quello della tabella join ready )	*/
	
	public int returnDim ()
	{ 
		return dim ;
	}

/*	Ritorna la tabella originale, di partenza 	*/
	
	public float[] returnTab ()
	{ 
		return L ;
	}

/*	Ritorna la nuova tabella creata ad hoc pronta ( "Ready" ) per svolgere l' equi join
	( viene aggiunto per ogni istanza di L.I l'attributo JAtt che è quello che verrà utilizzato per il join */ 
	
	public NuovaTabella returnTabJoinReady ()
	{ 
		return l ;
	}

}