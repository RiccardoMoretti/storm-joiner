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

					/* CLASS FOR TESTING */

public class TESTTupleCorretteSpurieDominioAttributoJoin {

	private final static Logger logger = LoggerFactory.getLogger(Test.class);

	private final static int NUMMARKERS  = 100;
	private final static int ONETWINEVERY  = 25;

	private final static int TUPLETABLEL  = 500;
	private final static int TUPLETABLER  = 500;
	
	private final static int DOMAINSTARTSAT  = 0;
	
	private final static int SOGLIA  = 5;

	private final static int NUMTESTCASE  = 25;

	public static void main(String[] args) throws Exception {
		
		float dataReal[] = new float[NUMTESTCASE];
		float dataSpur[] = new float[NUMTESTCASE];
		
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
		
		int DOMAINENDSAT  = 1000;
		
		for ( int i = 0 ; i < NUMTESTCASE ; i++ )
		{
			DOMAINENDSAT  = DOMAINENDSAT + 500;
						
			// create the data server
			DataServer ds = new DataServer(3000, "ThisIsASecretKey", markers, twin , DOMAINSTARTSAT, DOMAINENDSAT, SOGLIA);
			ds.last(4);
			ds.start();

			DataServerConnector sc1 = new DataServerConnector("tcp://127.0.0.1:3000", "1", Integer.toString(TUPLETABLEL));
			DataServerConnector sc2 = new DataServerConnector("tcp://127.0.0.1:3000", "1", Integer.toString (TUPLETABLER));

			// create the client
			Client client = new Client ("ThisIsASecretKey", markers, twin);
			client.connect("tcp://127.0.0.1:5555");
			
			client.join(sc1, sc2);
			
			dataReal[i] = Float.parseFloat(client.getDataReal());
			dataSpur[i] = Float.parseFloat(client.getDataSpur());
						
			client.destroy();

		}
		
		System.out.println("");
		logger.info("Domain from {} to {} ", DOMAINSTARTSAT, DOMAINENDSAT);
		logger.info("Number of tulpes L: {} ", TUPLETABLEL);
		logger.info("Number of tuples R: {} ", TUPLETABLER);
		System.out.println("");
			
		logger.info("\tDataReal\tDataSpur\tTotal\t\tErrPercent");
		
		for ( int i = 0 ; i < NUMTESTCASE ; i++ )			
			{ 
			  float total = dataReal[i]+dataSpur[i];
			  float errPerc = (dataSpur[i]/(dataReal[i]+dataSpur[i]))*100;
			  logger.info("\t{}\t\t{}\t\t{}\t\t{} %", dataReal[i], dataSpur[i], total , errPerc );
			}
				
		float NmaxSpur = 0;
		float NmaxReal = 0;
		float NmaxErrPerc = 0 ;
		
		for ( int i = 0 ; i < NUMTESTCASE ; i++ )			
		{ 
		  if ( dataSpur[i] > NmaxSpur )
		  {
			  NmaxSpur = dataSpur[i];
			  NmaxReal = dataReal[i];
			  NmaxErrPerc = (dataSpur[i]/(dataReal[i]+dataSpur[i]))*100 ;
		  }
		}
		
		System.out.println("");
		logger.info("Max number of spurious tuples \t{} spur\t{} real \t{} %", NmaxSpur, NmaxReal, NmaxErrPerc);
		
		float maxSpur = 0;
		float maxReal = 0;
		float maxErrPerc = 0 ;
		
		for ( int i = 0 ; i < NUMTESTCASE ; i++ )			
		{ 
		  if ( (dataSpur[i]/(dataReal[i]+dataSpur[i]))*100 > maxErrPerc )
		  {
			  maxSpur = dataSpur[i];
			  maxReal = dataReal[i];
			  maxErrPerc = (dataSpur[i]/(dataReal[i]+dataSpur[i]))*100 ;
		  }
		}
		
		System.out.println("");
		logger.info("Max number of Err percent \t{}%\t{} spur\t{} real ",maxErrPerc, maxSpur, maxReal);
	
		for ( int i = 0 ; i < NUMTESTCASE ; i++ )			
		{
		     String filename= "C:/Users/Moretti/Dropbox/Università/Tesi/Test/1TupleCorretteDominio.txt";
		     FileWriter fw = new FileWriter(filename,true); //the true will append the new data
			 fw.write(System.lineSeparator()+dataReal[i]);//appends the string to the file
			 fw.close();
		}
		
		for ( int i = 0 ; i < NUMTESTCASE ; i++ )			
		{
		     String filename= "C:/Users/Moretti/Dropbox/Università/Tesi/Test/1TupleSpurieDominio.txt";
		     FileWriter fw = new FileWriter(filename,true); //the true will append the new data
			 fw.write(System.lineSeparator()+dataSpur[i]);//appends the string to the file
			 fw.close();
		}
	
	}
}
