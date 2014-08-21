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

import java.io.*;

					/* CLASS FOR TESTING */

public class TESTDiscretizingTime {

	private final static Logger logger = LoggerFactory.getLogger(Test.class);

	private final static int THOUSAND = 1000;
	private final static int MILLION  = THOUSAND * THOUSAND;
	private final static int BILLION  = THOUSAND * MILLION;

	private final static int NUMMARKERS  = 50;
	private final static int ONETWINEVERY  = 10;

	private final static int DOMAINSTARTSAT  = 0;
	private final static int DOMAINENDSAT  = 250;

	private final static int TUPLETABLEL  = 5;
	private final static int TUPLETABLER  = 3;

	private final static int NUMTESTCASE  = 10;

	public static void main(String[] args) throws Exception {
		
		float elapsed[] = new float[NUMTESTCASE];
		long initial[] = new long[NUMTESTCASE];
		float discretizingTimeL[] = new float[NUMTESTCASE];
		float discretizingTimeR[] = new float[NUMTESTCASE];
		
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

			initial[i] = System.nanoTime();
			client.join(sc1, sc2);
			elapsed[i] = (System.nanoTime() - initial[i]) / ((float) BILLION);
			discretizingTimeL[i] = client.getDiscretizingTime1(); 
			discretizingTimeR[i] = client.getDiscretizingTime2();
			client.destroy();

		}
		
		System.out.println("");
		logger.info("Domain from {} to {} ", DOMAINSTARTSAT, DOMAINENDSAT);
		logger.info("Number of tulpes L: {} ", TUPLETABLEL);
		logger.info("Number of tuples R: {} ", TUPLETABLER);
		System.out.println("");
		logger.info("DiscretizationTimeL\tDiscretizationTimeR\tTotalTimeExeceution\tRapp");
		
		for ( int i = 0 ; i < NUMTESTCASE ; i++ )
			logger.info("\t{}\t\t{}\t\t{}\t\t{} %",discretizingTimeL[i],discretizingTimeR[i],elapsed[i],( (discretizingTimeL[i]+discretizingTimeR[i])/elapsed[i])*100 );
		
		for ( int i = 0 ; i < NUMTESTCASE ; i++ )			
		{
			String filenameL= "/Users/Riccardo Moretti/Dropbox/Università/Tesi/Test/2TempoDiscretizzazioneL.txt";
			FileWriter fwL = new FileWriter(filenameL,true); //the true will append the new data
			fwL.write(System.lineSeparator()+discretizingTimeL[i]);//appends the string to the file
			fwL.close();
		}
				
		for ( int i = 0 ; i < NUMTESTCASE ; i++ )
		{	
			String filenameR= "/Users/Riccardo Moretti/Dropbox/Università/Tesi/Test/2TempoDiscretizzazioneR.txt";
		    FileWriter fwR = new FileWriter(filenameR,true); //the true will append the new data
			fwR.write(System.lineSeparator()+discretizingTimeR[i]);//appends the string to the file
			fwR.close();
		}
		
		for ( int i = 0 ; i < NUMTESTCASE ; i++ )			
		{
		     String filenameT= "/Users/Riccardo Moretti/Dropbox/Università/Tesi/Test/2TempoTotale.txt";
		     FileWriter fwT = new FileWriter(filenameT,true); //the true will append the new data
			 fwT.write(System.lineSeparator()+elapsed[i]);//appends the string to the file
			 fwT.close();
		}
	
	}
}