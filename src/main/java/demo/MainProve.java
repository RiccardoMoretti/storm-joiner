
public class MainProve {
	
	public static void main(String[] args) 
	
	{	Read stream = new Read();
	
		float minimo, massimo, soglia ;

/* 	Leggo in input i valori del dominio ( valore minimo e massimo ) e il valore di soglia */
		
		System.out.println( "DOMINIO  : " ) ;
		
		System.out.println( "Inserire il valore minimo assumibile dall'attributo  : " ) ;
		minimo = stream.readFloat( ) ;
		
		System.out.println( "Inserire il valore massimo assumibile dall'attributo : " ) ;
		massimo = stream.readFloat( ) ;
		
		System.out.println( "Inserire il valore di soglia : " ) ;
		soglia = stream.readFloat( ) ;
		
/*	Creo la tabella L, passandogli i valori appena letti in input.
 * 	Divido il dominio in intervalli e discretizzo, ovvero associo ad ogni valore 
 *  continuo di L un valore discreto ( vedi spiegazione metodo classe ).
 * 	Per comodità e chiarezza poi stampo la tabella L di partenza e la nuova tabella
 *  con aggiunto l'attributo utile per poi fare l'equi- join. 	 */
	
		ImplementazioneL L = new ImplementazioneL ( minimo, massimo, soglia ) ;
		L.creaIntervalli(2);
		L.discretizza();
		
		int dimL; 
		float[] l ;
		NuovaTabella lj ;
		
		dimL = L.returnDim();
		l = new float [ dimL ];
		l = L.returnTab();
		
		lj = new NuovaTabella ( dimL ) ;
		lj = L.returnTabJoinReady() ;
	
		System.out.println( " " ) ;
		System.out.println( " TABELLA L ") ;
		for ( int h = 0 ; h < dimL ; h++ )
			System.out.println (" L.I :" + l[h] ) ;
		
		System.out.println( " " ) ;
		System.out.println( " TABELLA L EQUI-JOIN READY ") ;
		for ( int h = 0 ; h < dimL ; h++ )
			System.out.println (" L.I :" + lj.getAtt(h) + "\t L.JAtt :" + lj.getJAtt(h) ) ;
		
/*	Creo la tabella R, passandogli i valori di soglia e dominio letti in input.
 * 	Divido il dominio in intervalli e discretizzo, ovvero associo ad ogni valore 
 *  continuo di R due valori discreti ( vedi spiegazione metodo classe ).
 * 	Per comodità e chiarezza poi stampo la tabella R di partenza e la nuova tabella
 *  con aggiunto l'attributo utile per poi fare l'equi- join. 	 */		

		ImplementazioneR R = new ImplementazioneR (	minimo, massimo, soglia ) ;
		R.creaIntervalli(2);
		R.discretizza();
		
		int dimR, dimRJ ; 
		float[] r ;
		NuovaTabella rj ;
		
		dimR = R.returnDimTab();
		r = new float [ dimR ];
		r = R.returnTab();
		
		dimRJ = R.returnDimTabJoinReady() ;
		rj = new NuovaTabella ( dimRJ ) ;
		rj = R.returnTabJoinReady() ;
		
		System.out.println( " " ) ;
		System.out.println( " TABELLA R ") ;
		for ( int h = 0 ; h < dimR ; h++ )
			System.out.println (" R.I :" + r[h] ) ;
		
		System.out.println( " " ) ;
		System.out.println( " TABELLA R EQUI-JOIN READY ") ;
		for ( int h = 0 ; h < dimRJ ; h++ )
			System.out.println (" R.I :" + rj.getAtt(h) + "\t R.JAtt :" + rj.getJAtt(h) ) ;
				
/* 	Seguono una serie di operazioni utili per il testing/valutazione efficenza.
 * 	Calcolo del numero di tuple della tabella risultante senza tuple duplicate/di troppo.
 * 	Calcolo del numero di tuple della tabella effettivamente risultante dall'equi-join
 * 	 delle due tabelle create ad hoc con il metodo proposto.
 *	Calcolo delle tuple inutili/spure/di troppo da eliminare. 	*/
		
		int njoindistinct = 0 ;
		
		for ( int a = 0 ; a < L.returnDim() ; a ++ )
		{
			for ( int b = 0 ; b < R.returnDimTab() ; b ++ )
			{
				if ( Math.abs(l[a]-r[b]) <= soglia )
					njoindistinct++ ;
			}						
		}
		
		int njoin = 0 ;
		for ( int a = 0 ; a < L.returnDim() ; a ++ )
		{
			for ( int b = 0 ; b < R.returnDimTabJoinReady() ; b ++ )
			{
				if ( lj.getJAtt(a)== rj.getJAtt(b) )
					njoin++ ;
			}						
		}
		
		System.out.println( " " ) ;
		System.out.println( "APPROXIMATE JOIN " ) ;
		System.out.println( " " ) ;
		System.out.println( "Date le due relazioni L e R, composte rispettivamente da " + L.returnDim() + " e " + R.returnDimTab() + " tuple , il risultato dell'approximate join con soglia " + soglia + " è il seguente :" ) ;
		System.out.println( "Numero tuple joinate sul nuovo attributo ( con possibili ridondanze ) : " +njoin );
		System.out.println( "Numero tuple joinate distinct : " +njoindistinct );
		System.out.println( "Numero tuple spurie ( che il client deve rimuovere ): " + ( njoin - njoindistinct ) );
	
		System.out.println( "" ) ;		
		System.out.println( "Fine" ) ;
	}
}
	
