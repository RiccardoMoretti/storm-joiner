package demo;

import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import joiner.client.Client;
import joiner.commons.DataServerConnector;
import joiner.commons.twins.HashTwinFunction;
import joiner.commons.twins.TwinFunction;
import joiner.computational.ComputationalServer;
import joiner.server.DataServer;

						/* CLASS FOR TESTING */

public class Test {
	
	private final static Logger logger = LoggerFactory.getLogger(Test.class);

	private final static int THOUSAND = 1000;
	private final static int MILLION  = THOUSAND * THOUSAND;
	private final static int BILLION  = THOUSAND * MILLION;
	
	private final static int NUMMARKERS  = 100;
	private final static int ONETWINEVERY  = 10;
	
	private final static int DOMAINSTARTSAT  = 0;
	private final static int DOMAINENDSAT  = 50;
	
//	private final static int SOGLIA  = 1;
	
	private final static int TUPLETABLEL  = 10;
	private final static int TUPLETABLER  = 5;
	
	private final static int NUMTESTCASE  = 5;
	
	public static void main(String[] args) throws Exception {

		// create the markers 		
		Set<String> markers = new HashSet<String>();
		
		for (int i = 0; i < NUMMARKERS; ++i)
			markers.add(Integer.toString(i));

		// create the twin function ( one every 10 )
		TwinFunction twin = new HashTwinFunction(ONETWINEVERY);
				
		DataServerConnector sc1[] = new DataServerConnector[NUMTESTCASE];
		DataServerConnector sc2[] = new DataServerConnector[NUMTESTCASE];
	
		// create the computational server 
		ComputationalServer cs = new ComputationalServer(5555, 2);
		cs.start();

		// create the client 
		Client client[] = new Client[NUMTESTCASE];
				
		DataServer ds[] = new DataServer[5];

/*	L'idea è quella di eseguire il tutto NUMTESTCASE volte, ogni volta passando il valore di "i"
 * 		come valor di soglia ( agendo sempre su db con lo stesso numero di tuple ) e vedere
 * 		come cambia la percentuale di tuple spurie nel risultato.
 * 	Se eseguo manualmente il tutto ( eseguo con un valore di soglia, fermo l'esecuzione e la rilancio
 * 		con un altro valore ) funziona, se lo automatizzato ( come di seguito ) non funziona
 * 		ma non viene generato nessun errore quindi non so come comportarmi.
 *  Penso che il tutto sia dovuto alla mancata chiusura di qualche socket ma ho già controllato
 *  	e mi sembra apposto.. non so proprio cosa fare! 
 *  Penso che il problema sia nel client, in quanto alla seconda esecuzione si arriva fino alla "sua parte"
 *  	senza errori e poi si blocca tutto.
 */
		for ( int i = 1 ; i < NUMTESTCASE ; i++ )
		{
			// create the data server	
			ds[i] = new DataServer(3000, "ThisIsASecretKey", markers, twin , DOMAINSTARTSAT, DOMAINENDSAT, i);
			ds[i].start();
		
			sc1[i] = new DataServerConnector("tcp://127.0.0.1:3000", "1", Integer.toString(TUPLETABLEL));
			sc2[i] = new DataServerConnector("tcp://127.0.0.1:3000", "1", Integer.toString (TUPLETABLER));

			client[i] = new Client	("ThisIsASecretKey", markers, twin);
			client[i].connect("tcp://127.0.0.1:5555");
		
			long initial = System.nanoTime();
		
			client[i].join(sc1[i], sc2[i]);
		
			float elapsed = (System.nanoTime() - initial) / ((float) BILLION);
			logger.info("Elapsed time: {} s", elapsed);
		
			client[i].destroy();
			
		}	
	}	
}