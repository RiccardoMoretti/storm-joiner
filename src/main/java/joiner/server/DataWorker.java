package joiner.server;

import java.util.Set;

import javax.crypto.Cipher;


import joiner.commons.DataServerRequest;
import joiner.commons.Prefix;
import joiner.commons.twins.TwinFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

import joiner.computational.*;


public class DataWorker extends Thread {

	private final Logger logger = LoggerFactory.getLogger(DataWorker.class);

	private final int outputPort;
	private final Cipher cipher;
	private final Set<String> markers;
	private final TwinFunction twin;
	private final int moreFlag;
	private Socket socket;
	private float[] discretized ;
	private float[] random;
	private float rand;
	private int cont;
	private boolean ok;

	private ZContext context;
	private boolean done = false;
	private int from, to;
	private Domain D ;
	private ZipfGenerator Z;
	private String[] sendReady;
	
	public DataWorker(int outputPort, DataServerRequest request, Cipher cipher, Set<String> markers, TwinFunction twin) {
		this(outputPort, request, cipher, markers, twin, true);
	}

	public DataWorker(int outputPort, DataServerRequest request, Cipher cipher, Set<String> markers, TwinFunction twin, boolean pipeline) {
		this.outputPort = outputPort;
		this.cipher = cipher;
		this.markers = markers;
		this.twin = twin;
		this.moreFlag = pipeline ? 0 : ZMQ.SNDMORE;
		this.D = new Domain();
		this.Z = new ZipfGenerator ((int) D.getDomainSize(), 0.5 );
		this.discretized = new float [Integer.parseInt(request.getColumn())];
		this.random = new float [Integer.parseInt(request.getColumn())];
		this.rand = 0 ;
		this.cont = 0 ;
		this.ok = false ;
		this.sendReady = new String[Integer.parseInt(request.getColumn())] ;
		
		parseRequest(request);
	}

	private void parseRequest(DataServerRequest request) {
		// This is a fake DataWorker, it generates the data
		from = Integer.parseInt(request.getTable());
		to = Integer.parseInt(request.getColumn());
	}
	
	//return total number of tuples ( marker + twin + real )
	public int getRecordsHint() {
		return (int) (markers.size() + (to - from + 1) * (1 + twin.getPercent()));
	}

	@Override
	public void run() {

		context = new ZContext();
		socket = context.createSocket(ZMQ.PUSH);
		
		
		try {

			// Open the output socket
			socket.bind("tcp://*:" + outputPort);
			logger.info("Start pushing data to port {}", outputPort);

			
			// Send all the markers (without their twins) [TODO shuffle them with the data]
			for (String marker: markers)
			{	encryptAndSend(marker, marker, Prefix.MARKER, false); 
				//System.out.println("Marker \t" + marker );	
			}

	
			//l'attributo di join deve essere univoco ( chiave ), 
			// controllo che il genereatore li generi tutti diversi
		
			// Send the data (with the twins)
			for (int i = 0 ; i < to ; ++i)
		
			{	
				rand = Z.nextInt();
				ok = false ;
				//System.out.println("Rand\t\t " + rand );
				
				while ( !ok )
				{	 cont = 0;
		
					for ( int j = 0 ; j < i ; j++ )
						if ( random[j] != rand )
								cont++;
					
					if ( cont == i )
						ok = true;
					else
						rand = Z.nextInt() ;
				}
				
				random[i] = rand ;
				discretized[i] = this.discretized(random[i]);	
				//System.out.println("Original\t " + random[i]+"\tDiscretized\t" + discretized[i] );
			}
	
			
			for (int i = 0 ; i < to ; ++i)
				encryptAndSend( Float.toString(discretized[i]),Float.toString(random[i]), Prefix.DATA, true);

			// Signal the end of the connection
			socket.send("");
			logger.info("All data pushed to port {}", outputPort);

			while(!done)
				Thread.sleep(100);

		} catch ( Exception e ) {

			logger.error(e.getMessage());
			socket.send("SERVER ERROR: " + e.getMessage());

		} finally {
			context.destroy();
		}

	}



	private void encryptAndSend(String dataDisc, String dataReal, Prefix prefix, boolean addTwin) throws Exception {

		
		// send the message
		socket.send(cipher.doFinal((prefix.getPrefix() + dataDisc).getBytes("UTF-8"))+"\t"+cipher.doFinal((prefix.getPrefix() + dataReal).getBytes("UTF-8")), moreFlag);
		
		// send the twin if requested and needed
		if (addTwin && twin.neededFor(dataReal))
		{
			socket.send(cipher.doFinal((Prefix.TWIN.getPrefix() + dataDisc).getBytes("UTF-8"))+"\t"+cipher.doFinal((Prefix.TWIN.getPrefix() + dataReal).getBytes("UTF-8")), moreFlag);
		}
	}

	public float discretized ( float num )
	{
			float temp = D.getMax() ;
			float disc[] = new float[D.getN()] ;
			disc = D.getDisc();
			int index = 0;
			
			for ( int j = 0 ; j < D.getN() ; j++ )
			{
					if ( Math.abs( num - disc[j] ) < temp )
						{	temp = Math.abs( num - disc[j] );
							index = j ;
						}
			}	
			
			//System.out.println(" NUMERO "+num+"\t Intervallo "+disc[index]);
			return disc[index];			
	}
	
	
	public void done() {
		done = true;
	}

	
	@Override
	protected void finalize() throws Throwable {
		context.destroy();
		super.finalize();
	}

}
