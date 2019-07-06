import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;

import org.apache.log4j.*;

import org.apache.thrift.*;
import org.apache.thrift.server.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.*;

import org.apache.curator.*;
import org.apache.curator.retry.*;
import org.apache.curator.framework.*;
import org.apache.curator.framework.api.*;

public class KeyValueHandler implements KeyValueService.Iface, CuratorWatcher{
	private Map<String, String> myMap;
	private CuratorFramework curClient;
	private String zkNode;
	private String host;
	private int port;
	private static Logger log;
	private String primarySocket;
	private List<String> backupSockets;
	private ReentrantLock concurrencyLock = new ReentrantLock();

	public KeyValueHandler(String host, int port, CuratorFramework curClient, String zkNode) {
		this.host = host;
		this.port = port;
		this.curClient = curClient;
		this.zkNode = zkNode;
		myMap = new ConcurrentHashMap<String, String>();
		this.backupSockets = new ArrayList<String>();
		
		//set up log4j
		BasicConfigurator.configure();
		log = Logger.getLogger(KeyValueHandler.class.getName());
		
		// bind watcher
	    try {
	        this.curClient.getChildren().usingWatcher(this).forPath(this.zkNode);
	    } catch (Exception e) {
	        e.printStackTrace();
	    }
	}

	public String get(String key) throws org.apache.thrift.TException {
		String ret = myMap.get(key);
		if (ret == null)
			return "";
		else
			return ret;
	}

	public void put(String key, String value) throws org.apache.thrift.TException {
		myMap.put(key, value);
	}
	
	public void replicateData(Map<String, String> primaryData) {
		try {
			concurrencyLock.lock();
			myMap.putAll(primaryData);
		}catch(Exception e) {
			log.error("Failed to replicate data from primary node to backup node:" + this.host + ":" + this.port);
			e.printStackTrace();
		}finally {
			concurrencyLock.unlock();
		}
	}
	
	public Map<String, String> getMyMap() {
		return this.myMap;
	}
	
	/*** Copied from A3Client.java getPrimary() ***/
	private List<String> fetchZookeeperChildNodes() throws Exception {
		List<String> result;
		while (true) {
			this.curClient.sync();
			result = this.curClient.getChildren().usingWatcher(this).forPath(this.zkNode);
			if (result.size() == 0) {
				log.error("No primary found");
				Thread.sleep(100);
				continue;
			}
			Collections.sort(result);
			break;
		}
		
		return result;
	}
	
	private boolean isPrimary() {
		if(this.primarySocket == null) {
			log.error("This shouldn't happen at all");
			return false;
		}
		String primaryHost = primarySocket.split(":")[0];
		int primaryPort = Integer.parseInt(primarySocket.split(":")[1]);
		
		return this.host.equals(primaryHost) && this.port == primaryPort;
	}
	
	@Override
	synchronized public void process(WatchedEvent event) {
		log.info("Zookeeper event caught: " + event.toString());
		try {
			// fetch all children
			List<String> childNodes = fetchZookeeperChildNodes();
			log.info("The number of active child nodes: " + childNodes.size());
			log.info("The list of active child nodes:" + childNodes.toString());
			
			// memorize primary socket (it's the first one in the sorted list)
			this.primarySocket = new String(this.curClient.getData().forPath(this.zkNode + "/" + childNodes.get(0)));
			
			// memorize all other sockets as backup ones
			for(int idx = 1; idx < childNodes.size(); idx++) {
				this.backupSockets.add(new String(this.curClient.getData().forPath(this.zkNode + "/" + childNodes.get(idx))));
			}
			
			log.info("Current node is" + this.host + ":" + this.port + ". isPrimary: " + this.isPrimary());
			
			// if the current node is not primary node, will start to replicate data from primary node
			if(!isPrimary()) {
				// establish RPC to primary socket
				String primaryHost = primarySocket.split(":")[0];
				int primaryPort = Integer.parseInt(primarySocket.split(":")[1]);
				TSocket tSocket = new TSocket(primaryHost, primaryPort);
	            TTransport tTransport = new TFramedTransport(tSocket);
	            tTransport.open();
	            TProtocol tProtocol = new TBinaryProtocol(tTransport);
	            // retrieve primary client
	            KeyValueService.Client primaryClient = new KeyValueService.Client(tProtocol);
	            
	            // start to replicate data from primary socket to the current backup socket
	            this.replicateData(primaryClient.getMyMap());
	            log.info("Finished replicating data from primary node to backup node" + this.host + ":" + this.port);
	           // tTransport.close();
			}		
		}catch(InterruptedException e) {
			e.printStackTrace();
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
}
