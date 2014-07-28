package joiner.computational;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import joiner.commons.Bytes;

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
	private boolean skip;
	
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
		skip = false;
	}
	
	@Override
	public void run() {
		try {

			Set<Bytes> pendingKeys = new HashSet<Bytes>(entriesHint);
			logger.info("joiner created with entriesHint: {}", entriesHint);

			while (true) {
				Bytes message = new Bytes(input.recv());		
				
				if (message.isEmpty()) {
					output.send(message.getBytes());
					break;
				}

/*	
 * 				se "message" � un dato modificato allora devo fare il join usando lui
 * 				se "message" � la stringa di skip devo leggere il messaggio reale che 
 * 					viene dopo e se il join ( fatto sulla stringa modificata ) dava vero
 *					allora dovevo mandare la stringa reale al client. 
 *				Come posso fare ?					
*/	
					
				
				if (pendingKeys.contains(message)) {
						pendingKeys.remove(message);
						output.send(message.getBytes());
					} else
						pendingKeys.add(message);
				
			
			}
			
			input.disconnect(inputString);
			
			pendingKeys = null;
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
