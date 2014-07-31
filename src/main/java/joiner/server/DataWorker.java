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

	private int portClient;
	private final int outputPort;
	private final Cipher cipher;
	private float[] tempDisc;
	private final Set<String> markers;
	private final TwinFunction twin;
	private final int moreFlag;
	private Socket socket;
	private Socket socketClient;
	private String received[];

	private float[] discretized ;
	private float[] random;
	private float rand;
	private int cont;
	private int y;
	private boolean ok;

	private ZContext context;
	private ZContext contextClient;
	private boolean done = false;
	private int from, to;
	private Domain D ;
	private ZipfGenerator Z;

	
	public DataWorker(int outputPort, DataServerRequest request, Cipher cipher, Set<String> markers, TwinFunction twin, int port) {
		this(outputPort, request, cipher, markers, twin, true, port);
	}

	public DataWorker(int outputPort, DataServerRequest request, Cipher cipher, Set<String> markers, TwinFunction twin, boolean pipeline, int port) {
		this.outputPort = outputPort;
		this.cipher = cipher;
		this.markers = markers;
		this.twin = twin;
		this.moreFlag = pipeline ? 0 : ZMQ.SNDMORE;
		this.D = new Domain();
		this.Z = new ZipfGenerator ((int) D.getDomainSize(), 0.5 );
		this.discretized = new float [Integer.parseInt(request.getColumn())*2];
		this.random = new float [Integer.parseInt(request.getColumn())*2];
		this.received = new String [1000000];
		this.rand = 0 ;
		this.cont = 0 ;	
		this.ok = false ;
		this.y= 0;
		this.portClient = port;
		this.tempDisc = new float[2];
		
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
if ( outputPort % 2 != 0)
	{					
		try {

			// Open the output socket
			socket.bind("tcp://*:" + outputPort);
			logger.info("Start pushing data to port {}", outputPort);
			
			// Send all the markers (without their twins) [TODO shuffle them with the data]
			for (String marker: markers)
			{	encryptAndSend(marker, Prefix.MARKER, false); 
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

			}
	
			
			for (int i = 0 ; i < to ; ++i)
			{	//System.out.println("DISCRETIZZATO :\t"+discretized[i]);
				encryptAndSend( Float.toString(discretized[i]), Prefix.DATA, true);
			}
			
			// Signal the end of the connection
			socket.send("");
			logger.info("All data pushed to port {}", outputPort);

			while(!done)
				Thread.sleep(100);
			
			System.out.println( "\t\tRELAZIONE");
			for ( int u = 0 ; u < to ; u++ )
				System.out.println( "DISCRETIZZATO\t"+discretized[u]+"\tORIGINALE\t"+random[u]);

		} catch ( Exception e ) {

			logger.error(e.getMessage());
			socket.send("SERVER ERROR: " + e.getMessage());

		} finally {
						context.destroy();	
					}	
		
	}else
		
	{
		
		try {

			// Open the output socket
			socket.bind("tcp://*:" + outputPort);
			logger.info("Start pushing data to port {}", outputPort);
			
			// Send all the markers (without their twins) [TODO shuffle them with the data]
			for (String marker: markers)
			{	encryptAndSend(marker, Prefix.MARKER, false); 
				//System.out.println("Marker \t" + marker );	
			}
	
		
			// Send the data (with the twins)		
			
			for (int i = 0 ; i < 2*to ; i = i + 2 )
		
			{	rand = Z.nextInt()+D.getMin();
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
						rand = Z.nextInt()+D.getMin() ;
				}
				
				
				random[i] = rand ;
				random[i+1] = rand ;
				tempDisc = this.discretizedBis(random[i]);				
				discretized[i] = tempDisc[0];
				discretized[i+1] = tempDisc[1];
		
				//System.out.println("Original\t " + random[i]+"\tDiscretized\t" + discretized[i] );
				//System.out.println("Original\t " + random[i+1]+"\tDiscretized\t" + discretized[i+1] );
			
			}
	
			
			for (int i = 0 ; i < 2*to ; ++i)
			{	//System.out.println("DISCRETIZZATO :\t"+discretized[i]);
				encryptAndSend( Float.toString(discretized[i]), Prefix.DATA, true);
			}
			// Signal the end of the connection
			socket.send("");
			logger.info("All data pushed to port {}", outputPort);

			while(!done)
				Thread.sleep(100);
			
			System.out.println( "\t\tRELAZIONE");
			for ( int u = 0 ; u < 2*to ; u++ )
			System.out.println( "DISCRETIZZATO\t"+discretized[u]+"\tORIGINALE\t"+random[u]);

		} catch ( Exception e ) {

			logger.error(e.getMessage());
			socket.send("SERVER ERROR: " + e.getMessage());

		} finally {
						context.destroy();	
					}	
		
	}
		
		initClientDataServer();
		receiveRequest();	
		respondeRequest();
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

	private float discretized ( float num )
	{
			float temp = D.getMax() ;
			float disc[] = new float[D.getN()] ;
			disc = D.getDisc();
			int index = 0;
			
			for ( int j = 0 ; j < D.getN() ; j++ )
			{
					if ( Math.abs( num - disc[j] ) < temp )
						{	
							temp = Math.abs( num - disc[j] );
							index = j ;
						}
			}				
			
			//System.out.println(" NUMERO "+num+"\t Intervallo "+disc[index]);
			return disc[index];			
	}
	
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
					if ( Math.abs( ( num - D.getSoglia() ) - disc[j] ) < tempdown )
					{
						tempdown = Math.abs( ( num - D.getSoglia() ) - disc[j] ) ;
						indexdown = j ;
					}
			
					if ( Math.abs( ( num + D.getSoglia() ) - disc[j] ) < tempup )
					{
						tempup = Math.abs( ( num + D.getSoglia() ) - disc[j] ) ;
						indexup = j ;
					}
				}
			
			forReturn[0]= disc[indexdown] ;		
			forReturn[1]= disc[indexup] ;
			//System.out.println("NUMBER\t "+num+"\t"+disc[indexdown]+ "\t"+disc[indexup]);
			
		
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

	private void receiveRequest()
	{		
			while ( true )
					{					
						Bytes msg = new Bytes ( socketClient.recv() );
					
						if (msg.isEmpty())
							break;			
						
						received[y] = msg.toString();
						y++;
						//System.out.println(" RICHIESTO\t "+msg.toString());  
					}
	}

	private void respondeRequest()
	{	  
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
	
	@Override
	protected void finalize() throws Throwable {
		context.destroy();
		super.finalize();
	}

}
