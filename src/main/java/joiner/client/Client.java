package joiner.client;

import java.util.HashSet;
import java.util.Observable;
import java.util.Set;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import joiner.commons.Bytes;
import joiner.commons.DataServerConnector;
import joiner.commons.Prefix;
import joiner.commons.twins.TwinFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;

public class Client extends Observable {
	
	private final static Logger logger = LoggerFactory.getLogger(Client.class);
	
	private final ZContext context;
	private ZContext contextDataServer1;
	private ZContext contextDataServer2;
	

	private final Socket socket;
	private Socket  socketDataServer1;
	private Socket  socketDataServer2;
	private final Cipher cipher;
	
	private final Set<String> markers;
	private final TwinFunction twin;
	private String received1[] ;
	private String received2[] ;
	private int y ;
	private int g ;
	private int dataOK ;
	private String joinOK[] ;
	private float rcv1[];
	private float rcv2[];
	
	
	private Set<String> pendingTwins;
	private Set<String> pendingMarkers;
	private int receivedData;
	private int receivedTwins;
	private int outputPort1;
	private int outputPort2;
	private String request[];
	private int i;
	
	public Client(String key, Set<String> markers, TwinFunction twin) throws Exception {
		this.cipher = createCipher(key);
		this.markers = markers;
		this.twin = twin;
		this.context = new ZContext();
		this.socket = context.createSocket(ZMQ.PAIR);
		this.socket.setHWM(100000000);
		this.socket.setLinger(-1);
		this.i = 0 ;
		this.request = new String[1000000];
		this.received1 = new String[1000000];
		this.received2 = new String[1000000];
		this.rcv1 = new float[1000000];
		this.rcv2 = new float[1000000];
		this.y = 0 ;
		this.g = 0 ;
		this.dataOK = 0;
		this.joinOK = new String[1000000];
	}
	
	private Cipher createCipher(String key) throws Exception {
		Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
		SecretKey secretKey = new SecretKeySpec(key.getBytes("UTF-8"), "AES");
		cipher.init(Cipher.DECRYPT_MODE, secretKey);
		return cipher;
	}
	
	public void connect(String connectionString) {
		socket.connect(connectionString);
	}
	
	public void destroy() {
		context.destroy();
	}
	
	public void join(DataServerConnector ... connectors) throws Exception {
		
		// send the connectors to the computational server
		sendDataConnectors(connectors);
				
		// initialize the data structures
		pendingMarkers = new HashSet<String>(markers);
		pendingTwins = new HashSet<String>();
		receivedData = 0;
		receivedTwins = 0;
		
		// receive the messages and process them until completed
		while (true) {
			byte[] bytes = socket.recv();
			
			if (bytes.length == 0)
				break;
			
			processMessage(bytes);
		}
		
		socket.send("ACK");	
				
		initClientDataServer1();
		initClientDataServer2();
		
		for ( int u = 0 ; u < receivedData ; u++ )
			{// System.out.println( "\tDA RICHIEDERE\t"+request[u]); 
			  requestData1(request[u]);
			  requestData2(request[u]);
			  
			}
		requestData1("");
		requestData2("");
		receiveRequestData1();	
		receiveRequestData2();
		
		socketDataServer1.close();
		socketDataServer2.close();
		
		
		validateResult();
		checkSpuriosTuple();
	}
	
	private void sendDataConnectors(DataServerConnector[] connectors) {
		logger.info("starting join");
		for (DataServerConnector connector: connectors)
			socket.sendMore(connector.toMsg());
		socket.send("");
	}
	
	private void processMessage(byte[] bytes) throws Exception {
		// decipher the message
		String message = new String(cipher.doFinal(bytes), "UTF-8");
		
		// split the message in its prefix and its payload
		Prefix prefix = Prefix.of(message.charAt(0));
		String payload = message.substring(1);
		
		// process the prefix and the payload
		process(prefix, payload);
	}
	
	private void process(Prefix prefix, String payload) {
		switch (prefix) {
		
		case DATA:
			logger.debug("match: {}", payload);
			++receivedData;
			//System.out.println(" PREFISSO : "+prefix+"\tPAYLOAD : "+payload);
			request[i]= payload;
			i++;
				
			
			if (twin.neededFor(payload))
				xorSet(pendingTwins, payload);
			
			setChanged();
			notifyObservers(payload);
			break;

		case MARKER:
			logger.debug("marker: {}", payload);
			xorSet(pendingMarkers, payload);
			break;

		case TWIN:
			logger.debug("twin: {}", payload);
			++receivedTwins;
			xorSet(pendingTwins, payload);
			break;

		default:
			logger.error("UNEXPECTED RECEIVED DATA");
		}
	}
	
	private boolean validateResult() {
		// prevent short-circuit evaluation
		boolean valid = validateMarkers() & validateTwins();
		
		if (valid)
			{ logger.info("RESULT VALID. #RECEIVED: {}", receivedData);
				
			}
		else
			logger.info("RESULT NOT VALID. #RECEIVED: {}", receivedData);
		
		return valid;
	}
	
	private void checkSpuriosTuple() {
	
		String temp[];
		temp = new String[2];
		System.out.println("RICCARDO"+received1[2]);
	//	String[] parts = received1[2].split("\t");
		
	/*	for ( int r = 0 ; r < y ; r++ )
		{	String[] parts = received1[r].split("\t");
			rcv1[r] = Float.valueOf( parts[0]);
			
		}
			
			
		for ( int m = 0 ; m < g ; m++ )
			
		{	
			String[] parts = received2[m].split("\t");
			rcv2[m] = Float.valueOf( parts[0]);
		}	
		
		for ( int r = 0 ; r < y ; r++ )
				for ( int m = 0 ; m < g ; m++ )
					if ( Math.abs( rcv1[r] - rcv2[m] ) <= 5 )
						dataOK++;
			logger.info("TUPLE CORRETTE : {}", dataOK);
			logger.info("TUPLE DI TROPPO : {}", receivedData - dataOK);
			logger.info("PERCENTUALE TUPLE DI TROPPO: {}", ((receivedData - dataOK)/receivedData ));
			
		*/
	}

	private boolean validateMarkers() {
		logger.info("{} matched markers", markers.size() - pendingMarkers.size());
		logger.info("{} unmatched markers: {}", pendingMarkers.size(), pendingMarkers);
		return pendingMarkers.isEmpty();
	}
	
	private boolean validateTwins() {
		logger.info("{} matched twins", receivedTwins - pendingTwins.size());
		logger.info("{} missing twins: {}", pendingTwins.size(), pendingTwins);
		return pendingTwins.isEmpty();
	}
	
	private <T> void xorSet(Set<T> set, T elem) {
		if (set.contains(elem))
			set.remove(elem);
		else
			set.add(elem);
	}
	
	
	private void initClientDataServer1()
	{
		outputPort1 = 6002 ;
		contextDataServer1 = new ZContext();
		socketDataServer1 = contextDataServer1.createSocket(ZMQ.PAIR);
		socketDataServer1.bind("tcp://*:" + outputPort1);
		
	}
	
	private void requestData1(String payload)
	{							
				    socketDataServer1.send(payload);
	}
	
	private void receiveRequestData1()
	{
		while ( true )
			{
		
				Bytes msg = new Bytes ( socketDataServer1.recv() );
		
				if (msg.isEmpty())
					{
						break;
					}	
    	
				received1[y] = msg.toString();
				System.out.println(" DATA SERVER 1\t "+	received1[y] );
				g++;		
	}}
	
	private void initClientDataServer2()
	{
		outputPort2 = 6004 ;
		contextDataServer2 = new ZContext();
		socketDataServer2 = contextDataServer2.createSocket(ZMQ.PAIR);
		socketDataServer2.bind("tcp://*:" + outputPort2);
		
	}
	
	private void requestData2(String payload)
	{							
				    socketDataServer2.send(payload);
	}
	
	private void receiveRequestData2()
	{
		while ( true )
			{
		
				Bytes msg = new Bytes ( socketDataServer2.recv() );
		
				if (msg.isEmpty())
					{
						break;
					}	
    	
				received2[y] = msg.toString();
				System.out.println(" DATA SERVER 2\t "+msg.toString());
				y++;		
	}}

}
