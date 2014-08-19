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

public class TESTFilteringTime {

	private final static Logger logger = LoggerFactory.getLogger(Test.class);

	private final static int THOUSAND = 1000;
	private final static int MILLION  = THOUSAND * THOUSAND;
	private final static int BILLION  = THOUSAND * MILLION;

	private final static int NUMMARKERS  = 100;
	private final static int ONETWINEVERY  = 10;

	private final static int DOMAINSTARTSAT  = 0;
	private final static int DOMAINENDSAT  = 50;

	private final static int TUPLETABLEL  = 10;
	private final static int TUPLETABLER  = 5;

	private final static int NUMTESTCASE  = 5;

	public static void main(String[] args) throws Exception {
		
		Float elapsedChecking[] = new Float[NUMTESTCASE];
		Float total[] = new Float[NUMTESTCASE];
		Float rappTime[] = new Float[NUMTESTCASE];

		// create the markers
		Set<String> markers = new HashSet<String>();

		for (int i = 0; i < NUMMARKERS; ++i)
			markers.add(Integer.toString(i));

		// create the twin function ( one every 10 )
		TwinFunction twin = new HashTwinFunction(ONETWINEVERY);

		// create the computational server
		ComputationalServer cs = new ComputationalServer(5555, 2);
		cs.last(NUMTESTCASE);
		cs.start();

		for ( int i = 0 ; i < NUMTESTCASE ; i++ )
		{
			// create the data server
			DataServer ds = new DataServer(3000, "ThisIsASecretKey", markers, twin , DOMAINSTARTSAT, DOMAINENDSAT, i + 1);
			ds.last(4);
			ds.start();

			DataServerConnector sc1 = new DataServerConnector("tcp://127.0.0.1:3000", "1", Integer.toString(TUPLETABLEL));
			DataServerConnector sc2 = new DataServerConnector("tcp://127.0.0.1:3000", "1", Integer.toString (TUPLETABLER));

			// create the client
			Client client = new Client ("ThisIsASecretKey", markers, twin);
			client.connect("tcp://127.0.0.1:5555");

			
			long initial = System.nanoTime();
			client.join(sc1, sc2);
			
			total[i] = (System.nanoTime() - initial) / ((float) BILLION);
			elapsedChecking[i]=client.getElapsedChecking();
			rappTime[i]= elapsedChecking[i]/total[i];
			
			client.destroy();

		}
		
		System.out.println("");
		logger.info("Domain from {} to {} ", DOMAINSTARTSAT, DOMAINENDSAT);
		logger.info("Number of tulpes L: {} ", TUPLETABLEL);
		logger.info("Number of tuples R: {} ", TUPLETABLER);
		
		float maxRapp = 0 ;
		float maxRappFilt = 0 ;
		
		for ( int i = 0 ; i < NUMTESTCASE ; i++ )
		{			
			if ( rappTime[i] > maxRapp )
					{
						maxRapp = rappTime[i]; 
						maxRappFilt = elapsedChecking[i];
					}
		}
		
		logger.info("\tCheckingTime\tRappTime\t\t");
		
		for ( int i = 0 ; i < NUMTESTCASE ; i++ )
			logger.info("\t{} s\t\t\t{} %", elapsedChecking[i],rappTime[i]);
		
		logger.info("Max time used for filtering\t{} s\t\t{}%", maxRappFilt, maxRapp);
						
	}
}