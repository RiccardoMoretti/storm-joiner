package joiner.server;

import java.util.Set;


import javax.crypto.Cipher;

import joiner.commons.Bytes;
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
	
	private final static int THOUSAND = 1000;
	private final static int MILLION  = THOUSAND * THOUSAND;
	private final static int BILLION  = THOUSAND * MILLION;
	
	private final static int MAXDATA  = 1000000;
	
	private ZContext context;
	private ZContext contextClient;
	
	private Socket socket;
	private Socket socketClient;
	
	private int portClient;
	private final int outputPort;
		
	private final Cipher cipher;	
	private final Set<String> markers;
	private final TwinFunction twin;
	private final int moreFlag;
	
	private String received[];

	private float[] discretized ;
	private float[] random;
	private float[] tempDisc;
	private float rand;
	
	private int from, to;
	private int y;
	
	private boolean done = false;
	
	private ZipfGenerator Z;
	private Domain D ;
	
	private float discretizingTime;

	public DataWorker(int outputPort, DataServerRequest request, Cipher cipher, Set<String> markers, TwinFunction twin, int port, float min, float max, float soglia) {
		this(outputPort, request, cipher, markers, twin, true, port, min, max , soglia);
	}

	public DataWorker(int outputPort, DataServerRequest request, Cipher cipher, Set<String> markers, TwinFunction twin, boolean pipeline, int port, float min, float max, float soglia) {
		this.outputPort = outputPort;
		this.cipher = cipher;
		this.markers = markers;
		this.twin = twin;
		this.moreFlag = pipeline ? 0 : ZMQ.SNDMORE;		
		this.discretized = new float [Integer.parseInt(request.getColumn())*2];
		this.random = new float [Integer.parseInt(request.getColumn())*2];
		this.received = new String [MAXDATA];
		this.rand = 0 ;
		this.y= 0;
		this.portClient = port;
		this.tempDisc = new float[2];	
		this.D = new Domain(min,max,soglia);
		this.Z = new ZipfGenerator ((int) D.getDomainSize(), 0.5 );
		this.discretizingTime = 0 ;
				
		parseRequest(request);
	}

	private void parseRequest(DataServerRequest request) {
		// This is a fake DataWorker, it generates the data
		from = Integer.parseInt(request.getTable());
		to = Integer.parseInt(request.getColumn());
	}
	
	// return total number of tuples ( markerS + twinS + real )
	public int getRecordsHint() {
		return (int) (markers.size() + (to - from + 1) * (1 + twin.getPercent()));
	}

	@Override
	public void run() {

		context = new ZContext();
		socket = context.createSocket(ZMQ.PUSH);

		// the behavior for the two data server is different, we have to distinguish them
		
		// first data server
		if ( outputPort % 2 != 0)
		{					
			try {

				// Open the output socket
				socket.bind("tcp://*:" + outputPort);
				//////logger.info("Start pushing data to port {}", outputPort);
			
				// Send all the markers (without their twins) [TODO shuffle them with the data]
				for (String marker: markers)
					encryptAndSend(marker, Prefix.MARKER, false); 	
		
			// Send the data (with the twins)
			
				
				for (int i = 0 ; i < to ; ++i)	
				{	
					rand = Z.nextInt()+D.getMin();
					random[i] = rand ;
					long initial = System.nanoTime();				
					discretized[i] = this.discretized(random[i]);						
					float elapsed = (System.nanoTime() - initial) / ((float) BILLION);
					discretizingTime = discretizingTime + elapsed;
				}
				
	
				for (int i = 0 ; i < to ; ++i)
					encryptAndSend( Float.toString(discretized[i]), Prefix.DATA, true);
						
				// 	Signal the end of the connection
				socket.send("");
				logger.info("All data pushed to port {}", outputPort);

				while(!done)
					Thread.sleep(100);
			
			/*	System.out.println( "\t\tTABLE L");
				for ( int u = 0 ; u < to ; u++ )
					System.out.println( "Original:\t"+random[u]+"\tDiscretized:\t"+discretized[u]);
			 */
			} catch ( Exception e ) {

				logger.error(e.getMessage());
				socket.send("SERVER ERROR: " + e.getMessage());

			} finally {
						context.destroy();	
					  }	
			// second data server		
	}else
		{
		
			try {

				// Open the output socket
				socket.bind("tcp://*:" + outputPort);
				//////logger.info("Start pushing data to port {}", outputPort);
			
				// Send all the markers (without their twins) [TODO shuffle them with the data]
				for (String marker: markers)
					encryptAndSend(marker, Prefix.MARKER, false); 
					
				// Send the data (with the twins)		

				for (int i = 0 ; i < 2*to ; i = i + 2 )
		
				{	rand = Z.nextInt()+D.getMin();
				random[i] = rand ;
				random[i+1] = rand ;
				
				long initial = System.nanoTime();
				tempDisc = this.discretizedBis(random[i]);						
				discretized[i] = tempDisc[0];
				discretized[i+1] = tempDisc[1];
				float elapsed = (System.nanoTime() - initial) / ((float) BILLION);
				discretizingTime = discretizingTime + elapsed;
			}
			

			
			for (int i = 0 ; i < 2*to ; ++i)
				encryptAndSend( Float.toString(discretized[i]), Prefix.DATA, true);
			
			// Signal the end of the connection
			socket.send("");
			logger.info("All data pushed to port {}", outputPort);

			while(!done)
				Thread.sleep(100);
			
		/*	System.out.println( "\t\tTABLE R");
			for ( int u = 0 ; u < 2*to ; u++ )
				System.out.println( "Original:\t"+random[u]+"\tDiscretized:\t"+discretized[u]);
				*/
		} catch ( Exception e ) {

			logger.error(e.getMessage());
			socket.send("SERVER ERROR: " + e.getMessage());

		} finally { 
					context.destroy();	
				  }			
	}
		
		// open the socket for directly communicate with the client
		initClientDataServer();
		
		//receive the request from the client
		receiveRequest();	
		
		// responde at the client request
		respondeRequest();
		
		sendDiscretizingTime();
		
		// close the socket with the client
		destroyClientDataServer();
	}

	private void encryptAndSend(String data, Prefix prefix, boolean addTwin) throws Exception {
		
		// send the message
		socket.send(cipher.doFinal((prefix.getPrefix() + data).getBytes("UTF-8")), moreFlag);
		
		// send the twin if requested and needed
		if (addTwin && twin.neededFor(data))
		{
			socket.send(cipher.doFinal((Prefix.TWIN.getPrefix() + data).getBytes("UTF-8")), moreFlag);
		}
	}

	//Each tuple $t$ in \tab1\ is then associated with the discrete value $v$ nearest to $t$[\Jatt] 
	private float discretized ( float num )
	{
			float temp = D.getMax() ;
			float disc[] = new float[D.getN()] ;
			disc = D.getDisc();
			float index = 0;
			
			for ( int j = 0 ; j < D.getN() ; j++ )
			{
				if ( Math.abs ( num-disc[j]) < temp)
				{ 	temp = Math.abs(num-disc[j]);
					index = disc[j] ;
				}
			}				
			
			return index;			
	}
	
	// Each tuple $t$ in \rtab\ is instead associated with two discrete values $v_1$ and $v_2$,
	//	 which are the discrete values nearest to $t$[\Jatt]$-${\em range} and $t$[\Jatt]$+${\em range} 
	private float[] discretizedBis( float num )
	{
		float tempup = D.getMax();
		float tempdown = D.getMax();
		float[] forReturn = new float[2];
		int indexdown = 0 ;
		int indexup = 0 ;
		float disc[] = new float[D.getN()] ;
		disc = D.getDisc();
				
			for ( int j = 0 ; j < D.getN() ; j++ )
				{
					if ( Math.abs( ( num - D.getThreshold() ) - disc[j] ) <= tempdown )
					{
						tempdown = Math.abs( ( num - D.getThreshold() ) - disc[j] ) ;
						indexdown = j ;
					}
			
					if ( Math.abs( ( num + D.getThreshold() ) - disc[j] ) <= tempup )
					{
						tempup = Math.abs( ( num + D.getThreshold() ) - disc[j] ) ;
						indexup = j ;
					}
				}
			
			forReturn[0]= disc[indexdown] ;		
			forReturn[1]= disc[indexup] ;
					
		return forReturn;	
	}
	
	public void done() {
		done = true;
	}
	
	private void initClientDataServer()
	{
		contextClient = new ZContext();
		socketClient = contextClient.createSocket(ZMQ.PAIR);
		socketClient.connect("tcp://localhost:" + portClient );
	}

	private void destroyClientDataServer()
	{
		contextClient.destroy();
	}
	
	private void receiveRequest()
	{		
			while ( true )
					{					
						Bytes msg = new Bytes ( socketClient.recv() );
					
						if (msg.isEmpty())
							break;			
						
						received[y] = msg.toString();
						y++;
					}
	}

	private void respondeRequest()
	{	
		// distinguish the two different data server
		if ( outputPort % 2 != 0)
		{
			for ( int k = 0 ; k < y ; k++ )
					for ( int q = 0 ; q < to ; q++)
						if ( received[k].equals(Float.toString(discretized[q]))) 					  
							socketClient.send(Float.toString(random[q])+"\t"+ Float.toString(discretized[q]));
		}
		else
		{
			for ( int k = 0 ; k < y ; k++ )
				for ( int q = 0 ; q < 2*to ; q++)
					if ( received[k].equals(Float.toString(discretized[q]))) 					  
						socketClient.send(Float.toString(random[q])+"\t"+ Float.toString(discretized[q]));			
		}
			socketClient.send("");
	}
	
	private void sendDiscretizingTime()
	{	
			socketClient.send(Float.toString(discretizingTime));
			socketClient.send("");
	}
	
	@Override
	protected void finalize() throws Throwable {
		context.destroy();
		super.finalize();
	}

	public float getDiscretizingTime()
	{ 	return discretizingTime; 	}

}
