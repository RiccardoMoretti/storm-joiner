package demo;

import java.io.FileWriter;
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

						/* DA ESEGUIRE CON PARAMETRO 1 */

/*
* Dominio fisso 0-25000. Uso di 250 marker e 1 twin ogni 100. Soglia 0-50.
* 
* Far variare la cardinalità delle tabelle in vari run
* 
* 1000	FATTO
* 
* 5000 FATTO
* 
* 2500 fATTO
* 
*/

public class TESTFinalJoinTime {

	private final static Logger logger = LoggerFactory.getLogger(Test.class);

	private final static int THOUSAND = 1000;
	private final static int MILLION  = THOUSAND * THOUSAND;
	private final static int BILLION  = THOUSAND * MILLION;

	private final static int NUMMARKERS  = 250;
	private final static int ONETWINEVERY  = 100;

	private final static int DOMAINSTARTSAT  = 0;
	private final static int DOMAINENDSAT  = 25000;

	private final static int TUPLETABLEL  = 2500;
	private final static int TUPLETABLER  = 2500;

	private final static int NUMTESTCASE  = 50;
	
	public static void main(String[] args) throws Exception {
		
		Float timeCommunication[] = new Float[NUMTESTCASE];
		Float timeComputation[] = new Float[NUMTESTCASE];
		Float timeFinalJoin[] = new Float[NUMTESTCASE];
		Float total[] = new Float[NUMTESTCASE];

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
			
			timeCommunication[i]= client.getElapsedFinalJoinCommunication();
			timeComputation[i]= client.getElapsedFinalJoinComputation();
			timeFinalJoin[i]= client.getTotalFinalJoinTime();
			
			client.destroy();

		}
		
		System.out.println("");
		logger.info("Domain from {} to {} ", DOMAINSTARTSAT, DOMAINENDSAT);
		logger.info("Number of tulpes L: {} ", TUPLETABLEL);
		logger.info("Number of tuples R: {} ", TUPLETABLER);
		System.out.println("");
		
		float maxFinalJoin = 0 ;
		float maxRapp = 0 ;
		
		for ( int i = 0 ; i < NUMTESTCASE ; i++ )
		{			
			if ( timeFinalJoin[i]/total[i] > maxRapp )
					{
						maxFinalJoin = timeFinalJoin[i]; 
						maxRapp = timeFinalJoin[i]/total[i];
					}
		}
		
		logger.info("\tCommunicationTime\tComputationTime\t\tTotal\t\tRapp");
		
		for ( int i = 0 ; i < NUMTESTCASE ; i++ )
			logger.info("\t{} s\t\t{} s\t\t{} s \t{} %", timeCommunication[i], timeComputation[i], timeFinalJoin[i], timeFinalJoin[i]/total[i]);
		
		System.out.println("");
		logger.info("Max time used for final join\t{}\t{}%", maxFinalJoin, maxRapp);
				
		for ( int i = 0 ; i < NUMTESTCASE ; i++ )			
		{
		     String filename= "C:/Users/Moretti/Dropbox/Università/Tesi/Test/6CommunicationTime.txt";
		     FileWriter fw = new FileWriter(filename,true); //the true will append the new data
			 fw.write(System.lineSeparator()+timeCommunication[i]);//appends the string to the file
			 fw.close();
		}
		
		for ( int i = 0 ; i < NUMTESTCASE ; i++ )			
		{
		     String filename= "C:/Users/Moretti/Dropbox/Università/Tesi/Test/6ComputationTime.txt";
		     FileWriter fw = new FileWriter(filename,true); //the true will append the new data
			 fw.write(System.lineSeparator()+timeComputation[i]);//appends the string to the file
			 fw.close();
		}
		
		for ( int i = 0 ; i < NUMTESTCASE ; i++ )			
		{
		     String filename= "C:/Users/Moretti/Dropbox/Università/Tesi/Test/6FinalJoinTime.txt";
		     FileWriter fw = new FileWriter(filename,true); //the true will append the new data
			 fw.write(System.lineSeparator()+timeFinalJoin[i]);//appends the string to the file
			 fw.close();
		}
		
		for ( int i = 0 ; i < NUMTESTCASE ; i++ )			
		{
		     String filename= "C:/Users/Moretti/Dropbox/Università/Tesi/Test/6TempoTotale.txt";
		     FileWriter fw = new FileWriter(filename,true); //the true will append the new data
			 fw.write(System.lineSeparator()+total[i]);//appends the string to the file
			 fw.close();
		}
		
	}
}
