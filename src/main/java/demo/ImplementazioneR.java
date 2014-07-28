
public class ImplementazioneR {

/* 	Dichiarazione variabili utili per implementare l'algoritmo proposto.	*/
	
	Read re = new Read () ;						//Utile per gestire l'I/O
	float R[], disc[], vmin, vmax, soglia ;
	int dim , n ;
	NuovaTabella r;

/* 	Metodo costruttore che inizializza l'oggetto con i valori del dominio e di soglia passati 
 * 	come parametri d'ingresso.
 * 	Inoltre permette di creare la tabella, ricevendo in input il numero di tuple e i
 * 	valori delle varie istanze di R.I.		*/

	public ImplementazioneR( float min, float max, float s )
	{	    			
		// Creazione tabella di partenza
		 	System.out.println( "Inserire il numero di tuple della tabella : " ) ;
		 	dim = re.readInt( ) ;
		 	
		 	R = new float[ dim ] ;
			
		// Inserimento varie istanze ( tuple della tabella )	
			for ( int u = 0 ; u < dim ; u++ )
			{
				System.out.println( "Inserire l'elemento numero " + ( u + 1 ) + " della tabella : " ) ;
				R[ u ] = re.readFloat( ) ;
			}
		
		// Inizializzazione dominio e soglia
			vmin = min ;
			vmax = max ;
			soglia = s ;
		
		// Creazione oggetto utile in seguito	
			r = new NuovaTabella ( dim , R, vmax, 'R' ) ;
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
		
		disc = new float [ n ] ;							// Creo array numero discreti
		
		for ( float i = vmin ; i < vmax ; i = i + inc )
		{
			disc [ j ] = i ;
			j++ ; 		
		}	
		
		if ( ( disc[n-1] ) == 0 )
			disc [ j ] = vmax ;
		
/*		System.out.println ( " ETICHETTE INTERVALLI " ) ;
		System.out.println ( " Numero : " + n ) ;
		for ( int q = 0 ; q < n ; q ++ )
			System.out.println ( " ETICHETTA " + disc[q] ) ; 	*/
	}
	
/* 	Meotodo chiave che implementa il concetto fondamentale dell'algoritmo proposto.
 * 	Ogni istanza diRL.I viene associata ai due valori discreti ( etichetta intervallo ) più vicini
 * 	a sestesso - soglia e sestesso + soglia ( vedi spiegazione metodo classe ).
 * 	Questa associazione introduce un nuovo attributo JAtt che sarà quello su cui si baserà l'equi join.
 * 	Il numero di tuple della nuova tabella creata è uguale a quello della tabella di partenza.	 */
	
	public void discretizza ()
	{		
			float tempup = vmax;
			float tempdown = vmax;
			int indexdown = 0 ;
			int indexup = 0 ;
			int cont = 0 ;
			
		for ( int i = 0 ; i < dim ; i++ )
		{
			indexdown = 0 ;
			tempdown = vmax ;
			indexup = 0 ;
			tempup = vmax ;
			
			for ( int j = 0 ; j < n ; j++ )
			{
					if ( Math.abs( ( R[i] - soglia ) - disc[j] ) < tempdown )
					{
						tempdown = Math.abs( ( R[i] - soglia ) - disc[j] ) ;
						indexdown = j ;
					}
					
					if ( Math.abs( ( R[i] + soglia ) - disc[j] ) < tempup )
					{
						tempup = Math.abs( ( R[i] + soglia ) - disc[j] ) ;
						indexup = j ;
					}
			}
			
			r.setAtt ( cont , R[i] ) ;
			r.setJAtt( cont, disc [indexdown] ) ;
			
			r.setAtt ( cont + 1 , R[i] ) ;
			r.setJAtt( cont + 1 , disc [indexup] ) ;
			
			cont = cont + 2 ; 
									
		}		
	}

/*	Ritorna il numero di tuple della tabella di partenza.	*/
	public int returnDimTab ()
	{ 
		return dim ;
	}

/*	Ritorna la tabella originale, di partenza 	*/
	public float[] returnTab ()
	{ 
		return R ;
	}
	
/*	Ritorna il numero di tuple della tabella creata " ad hoc " per l'equi-join.		*/	
	public int returnDimTabJoinReady()
	{
		return 2 * dim ; 		
	} 

/*	Ritorna la nuova tabella creata ad hoc pronta ( "Ready" ) per svolgere l' equi join
	( viene aggiunto per ogni istanza di R.I l'attributo JAtt che è quello che verrà utilizzato per il join */
	
	public NuovaTabella returnTabJoinReady ()
	{ 
		return r ;
	}
	
}
