package joiner.computational;

import java.util.HashSet;
import java.util.Set;

import joiner.commons.Bytes;
import joiner.commons.DataServerRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

public class ComputationalWorker extends Thread {
	
	private static final Logger logger = LoggerFactory.getLogger(ComputationalWorker.class);
	
	private final ZContext context;
	private final String inputString;
	private final Socket input;
	private final Socket output;
	private final int entriesHint;
	private boolean done;
	
	public ComputationalWorker(String inputString, String outputString, int entriesHint) {
		this.inputString = inputString;
		this.entriesHint = entriesHint;
		
		context = new ZContext();
		input = context.createSocket(ZMQ.PULL);
		input.bind(inputString);
		
		output = context.createSocket(ZMQ.PUSH);
		output.setLinger(-1);
		output.connect(outputString);
		
		done = false;
	}
	
	@Override
	public void run() {
		try {

			Set<Bytes> pendingKeys = new HashSet<Bytes>(entriesHint);
			/*String[] discretized = new String[entriesHint];
			String[] reals = new String[entriesHint];
			int i = 0 ;
			boolean last = false;*/
			
			logger.info("joiner created with entriesHint: {}", entriesHint);

			while (true) {
				
				Bytes message = new Bytes(input.recv());	
				
				if (message.isEmpty())
				{
					//last = true;
					output.send(message.getBytes());
					break;
				}
					
				
			/*	String forSplit = message.toString();
				
				String[] parts = forSplit.split("\t");
				System.out.println( "PART[0]\t"+parts[0]+"\tPART[1]\t"+parts[1]);
				
				if ( !last )
				{	System.out.println("SONO QUI \t SONO QUI");
					discretized[i] = parts[0];
					reals[i] = parts[1];
					i++;
				}
				
				else
				{	
					for ( int j = 0 ; j < i ; j++ )
					{	System.out.println("\t"+discretized[j]+"\t"+parts[0]);
						if ( discretized[j].equals(parts[0]) )
							{
								output.send((reals[j]+"\t"+parts[1]).getBytes());					
								System.out.println("\t"+discretized[j]+"\t"+parts[0]);
							}
					}
				
				}
				//Bytes disc = new Bytes (  parts[0].getBytes("UTF-8") );
				//Bytes real = new Bytes ( parts[1].getBytes("UTF-8" ) );
				
				
			*/
				
				
			
				//VECCHIO FUNZIONANTE
			 
			  if (pendingKeys.contains(message)) {
			 
						pendingKeys.remove(message);
						output.send(message.getBytes());
					} else
						pendingKeys.add(message);
				
			
			}
			
			input.disconnect(inputString);
			
			//pendingKeys = null;
			System.gc();
			
			while (!done)
				Thread.sleep(100);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			context.destroy();
		}
		
	}
	
	public void done() {
		done = true;
	}

}
