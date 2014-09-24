package demo;

import java.util.HashSet;
import java.util.Set;

import joiner.client.Client;
import joiner.commons.DataServerConnector;
import joiner.commons.twins.HashTwinFunction;
import joiner.commons.twins.TwinFunction;
import joiner.computational.ComputationalServer;
import joiner.server.DataServer;

import java.io.*;

/*
* Dominio fisso 0-25000. Uso di 250 marker e 1 twin ogni 100. Soglia 0-50.
* 
* Far variare la cardinalità delle tabelle in vari run
* 
* 1000 
* 
* 2500	
* 
* 5000   
* 
*/
public class TESTCompleto {

	private final static int THOUSAND = 1000;
	private final static int MILLION  = THOUSAND * THOUSAND;
	private final static int BILLION  = THOUSAND * MILLION;
	
	private final static int NUMMARKERS  = 250;
	private final static int ONETWINEVERY  = 100;

	private final static int DOMAINSTARTSAT  = 0;
	private final static int DOMAINENDSAT  = 20000;
	
	private final static int TUPLETABLEL  = 1000;
	private final static int TUPLETABLER  = 1000;

	private final static int NUMTESTCASE  = 50;

	public static void main(String[] args) throws Exception {
		
		Float checkingTime[] = new Float[NUMTESTCASE];
		Float finalJoinTime[] = new Float[NUMTESTCASE];
		Float totalTimeTecniche[] = new Float[NUMTESTCASE];
		Float totalTime[] = new Float[NUMTESTCASE];
		Float discretizingTimeL[] = new Float[NUMTESTCASE];
		Float discretizingTimeR[] = new Float[NUMTESTCASE];
		Float discretizingTime[] = new Float[NUMTESTCASE];
		Float timeCommunication[] = new Float[NUMTESTCASE];
		Float timeComputation[] = new Float[NUMTESTCASE];
		Float timeFinalJoin[] = new Float[NUMTESTCASE];
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
		
		
		for ( int i = 0 ; i < NUMTESTCASE ; i++ )
		{
			
			long initial = System.nanoTime();
			// create the data server
			DataServer ds = new DataServer(3000, "ThisIsASecretKey", markers, twin , DOMAINSTARTSAT, DOMAINENDSAT, i + 1);
			ds.last(4);
			ds.start();

			DataServerConnector sc1 = new DataServerConnector("tcp://127.0.0.1:3000", "1", Integer.toString(TUPLETABLEL));
			DataServerConnector sc2 = new DataServerConnector("tcp://127.0.0.1:3000", "1", Integer.toString (TUPLETABLER));

			// create the client
			Client client = new Client ("ThisIsASecretKey", markers, twin);
			client.connect("tcp://127.0.0.1:5555");
	
			client.join(sc1, sc2);
			
			discretizingTimeL[i] = client.getDiscretizingTime1(); 
			discretizingTimeR[i] = client.getDiscretizingTime2();
			
			discretizingTime[i] = discretizingTimeL[i] + discretizingTimeR[i];
				
			timeCommunication[i]= client.getElapsedFinalJoinCommunication();
			timeComputation[i]= client.getElapsedFinalJoinComputation();
			timeFinalJoin[i]= client.getTotalFinalJoinTime();
		
			dataReal[i] = Float.parseFloat(client.getDataReal());
			dataSpur[i] = Float.parseFloat(client.getDataSpur());
			
			checkingTime[i] =client.getElapsedChecking();
			
			finalJoinTime[i] = client.getTotalFinalJoinTime();
			
			totalTimeTecniche[i] = discretizingTimeL[i] + discretizingTimeR[i] + finalJoinTime[i] + checkingTime[i];
			
			totalTime[i] = (System.nanoTime() - initial) / ((float) BILLION);
			
			client.destroy();

		}
		
		for ( int i = 0 ; i < NUMTESTCASE ; i++ )			
		{
		     String filename= "C:/Users/Moretti/Dropbox/Università/Tesi/Test/TempoDiscretizing.txt";
		     FileWriter fw = new FileWriter(filename,true); //the true will append the new data
			 fw.write(System.lineSeparator()+discretizingTime[i]);//appends the string to the file
			 fw.close();
		}
		
		
		
		for ( int i = 0 ; i < NUMTESTCASE ; i++ )			
		{
		     String filename= "C:/Users/Moretti/Dropbox/Università/Tesi/Test/CommunicationTime.txt";
		     FileWriter fw = new FileWriter(filename,true); //the true will append the new data
			 fw.write(System.lineSeparator()+timeCommunication[i]);//appends the string to the file
			 fw.close();
		}
		
		for ( int i = 0 ; i < NUMTESTCASE ; i++ )			
		{
		     String filename= "C:/Users/Moretti/Dropbox/Università/Tesi/Test/ComputationTime.txt";
		     FileWriter fw = new FileWriter(filename,true); //the true will append the new data
			 fw.write(System.lineSeparator()+timeComputation[i]);//appends the string to the file
			 fw.close();
		}
		
		for ( int i = 0 ; i < NUMTESTCASE ; i++ )			
		{
		     String filename= "C:/Users/Moretti/Dropbox/Università/Tesi/Test/RicombinazioneTime.txt";
		     FileWriter fw = new FileWriter(filename,true); //the true will append the new data
			 fw.write(System.lineSeparator()+timeFinalJoin[i]);//appends the string to the file
			 fw.close();
		}
		
		for ( int i = 0 ; i < NUMTESTCASE ; i++ )			
		{
		     String filename= "C:/Users/Moretti/Dropbox/Università/Tesi/Test/TempoFiltering.txt";
		     FileWriter fw = new FileWriter(filename,true); //the true will append the new data
			 fw.write(System.lineSeparator()+checkingTime[i]);//appends the string to the file
			 fw.close();
		}
		

		for ( int i = 0 ; i < NUMTESTCASE ; i++ )			
		{
		     String filename= "C:/Users/Moretti/Dropbox/Università/Tesi/Test/TempoTecniche.txt";
		     FileWriter fw = new FileWriter(filename,true); //the true will append the new data
			 fw.write(System.lineSeparator()+totalTimeTecniche[i]);//appends the string to the file
			 fw.close();
		}
		
		
		for ( int i = 0 ; i < NUMTESTCASE ; i++ )			
		{
		     String filename= "C:/Users/Moretti/Dropbox/Università/Tesi/Test/TempoTotale.txt";
		     FileWriter fw = new FileWriter(filename,true); //the true will append the new data
			 fw.write(System.lineSeparator()+totalTime[i]);//appends the string to the file
			 fw.close();
		}
		
		for ( int i = 0 ; i < NUMTESTCASE ; i++ )			
		{
		     String filename= "C:/Users/Moretti/Dropbox/Università/Tesi/Test/TupleCorrette.txt";
		     FileWriter fw = new FileWriter(filename,true); //the true will append the new data
			 fw.write(System.lineSeparator()+dataReal[i]);//appends the string to the file
			 fw.close();
		}
		
		for ( int i = 0 ; i < NUMTESTCASE ; i++ )			
		{
		     String filename= "C:/Users/Moretti/Dropbox/Università/Tesi/Test/TupleSpurie.txt";
		     FileWriter fw = new FileWriter(filename,true); //the true will append the new data
			 fw.write(System.lineSeparator()+dataSpur[i]);//appends the string to the file
			 fw.close();
		}

	}
}
