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

						/* EXAMPLE OF EXECUTION */

public class Demo {
	
	private final static Logger logger = LoggerFactory.getLogger(Demo.class);

	private final static int THOUSAND = 1000;
	private final static int MILLION  = THOUSAND * THOUSAND;
	private final static int BILLION  = THOUSAND * MILLION;
	
	private final static int NUMMARKERS  = 50;
	private final static int ONETWINEVERY  = 5;
	
	private final static int DOMAINSTARTSAT  = 0;
	private final static int DOMAINENDSAT  = 100;
	
	private final static float TRESHOLD  = ( float ) 1.5 ;
	
	private final static int TUPLETABLEL  = 50;
	private final static int TUPLETABLER  = 25;
	
	public static void main(String[] args) throws Exception {

		// create the markers
		Set<String> markers = new HashSet<String>();
		for (int i = 0; i < NUMMARKERS; ++i)
			markers.add(Integer.toString(i));

		// create the twin function
		TwinFunction twin = new HashTwinFunction(ONETWINEVERY);

		// create the data server
		DataServer ds = new DataServer(3000, "ThisIsASecretKey", markers, twin, DOMAINSTARTSAT, DOMAINENDSAT, TRESHOLD);
		ds.start();

		ComputationalServer cs = new ComputationalServer(5555, 2);
		cs.start();

		// create the client and execute the query
		Client client = new Client("ThisIsASecretKey", markers, twin);
		client.connect("tcp://127.0.0.1:5555");

		DataServerConnector sc1 = new DataServerConnector("tcp://127.0.0.1:3000", "1", Integer.toString(TUPLETABLEL));
		DataServerConnector sc2 = new DataServerConnector("tcp://127.0.0.1:3000", "1", Integer.toString(TUPLETABLER));
		
		long initial = System.nanoTime();
		
		client.join(sc1, sc2);
		
		float elapsed = (System.nanoTime() - initial) / ((float) BILLION);
		logger.info("Elapsed time: {} s", elapsed);
		
		client.destroy();
	}
	
}
	
