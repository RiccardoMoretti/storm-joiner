package joiner.server;

import java.util.HashMap;

import java.util.Map;
import java.util.Set;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import joiner.commons.DataServerRequest;
import joiner.commons.twins.TwinFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

public class DataServer extends Thread {

	private final static Logger logger = LoggerFactory.getLogger(DataServer.class);
	
	private final int helloPort;
	private final Set<String> markers;
	
	private final TwinFunction twin;	
	private final boolean pipeline;
	private int lastUsedPort;
	private Socket helloSocket;
	private float min;
	private float max;
	private float soglia;

	private Cipher cipher;

	private Map<Integer, DataWorker> workers;

	private ZContext context;
	private int last = -1;
	
	public DataServer(int helloPort, String key, Set<String> markers, TwinFunction twin, float min, float max, float soglia) throws Exception {
		this(helloPort, key, markers, twin, true, min, max, soglia);
	}
	
	public DataServer(int helloPort, String key, Set<String> markers, TwinFunction twin, boolean pipeline, float min, float max, float soglia) throws Exception {
		this.helloPort = helloPort;
		this.lastUsedPort = helloPort;
		this.markers = markers;
		this.twin = twin;
		this.cipher = createCipher(key);
		this.workers = new HashMap<Integer, DataWorker>(); 
		this.pipeline = pipeline;
		this.min = min;
		this.max=max;
		this.soglia=soglia;
	}
	
	public void last(int last) {
		this.last = last;
	}
	
	@Override
	public void run() {

	// Creates a new managed socket within this ZContext instance.
		context = new ZContext();
		
	//	using REP it uses REQ as socket type, connect, sends a message 
	//		and waits for one reply:
		helloSocket = context.createSocket(ZMQ.REP);
		
		try {
			
			// create the hello socket
			helloSocket.bind("tcp://*:" + helloPort);
			logger.info("DataServer UP");
			
			// receive the incoming requests
			while (last != 0) {
				receiveMessage();
				if (last > 0) --last;
			}
			
		} catch ( Exception e ) {
			
			logger.error(e.getMessage());
			
		} finally {
			
			context.destroy();
			logger.info("DataServer DOWN");
			
		}
	}

	private Cipher createCipher(String key) throws Exception {
		Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
		SecretKey secretKey = new SecretKeySpec(key.getBytes("UTF-8"), "AES");
		cipher.init(Cipher.ENCRYPT_MODE, secretKey);
		return cipher;
	}
	
	private void receiveMessage() throws Exception, Exception {
		String message = new String(helloSocket.recv());
		
		if (message.startsWith("DONE"))
			closeSocket(Integer.parseInt(message.split(" ")[1]));
		else
			receiveRequest(DataServerRequest.fromMsg(message));
				
	}
	
	private void receiveRequest(DataServerRequest request) {
		logger.info("Received request: {}", request);
		int port = ++lastUsedPort;
		
		DataWorker worker = new DataWorker(lastUsedPort, request, cipher, markers, twin, pipeline, 2*lastUsedPort, min, max, soglia);
		helloSocket.send(port + " " + worker.getRecordsHint());
		
		worker.start();
		workers.put(port, worker);
	}
	
	
	private void closeSocket(int port) throws InterruptedException {
		logger.info("Received done for port: {}", port);
		helloSocket.send("ACK");
		Thread.sleep(100);
		DataWorker worker = workers.remove(port);
		worker.done();
	}
	
}
