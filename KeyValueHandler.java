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
//	private ReentrantLock concurrencyLock = new ReentrantLock();
	private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();

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
		if(!this.isPrimary()) {
			log.error("Something is wrong - get method shouldnt be called by backup node");
			throw new org.apache.thrift.TException("Something is wrong - get method shouldnt be called by backup node");
		}
		
		try {
			//acquire read lock, the thread will be blocked until the lock can be acquired
			this.readWriteLock.readLock().lock();	
			String ret = myMap.get(key);
			if (ret == null)
				return "";
			else
				return ret;
		}catch(Exception e) {
			log.error("Error reading from MyMap.");
			e.printStackTrace();
		}finally {
			// release the read lock no matter what
			this.readWriteLock.readLock().unlock();
		}
		
		// if everything goes well the execution flow should never reach here 
		throw new TException("get operation failed");	
	}

	public void put(String key, String value) throws org.apache.thrift.TException {
		if(!this.isPrimary()) {
			log.error("Something is wrong - put method shouldnt be called by backup node");
			throw new org.apache.thrift.TException("Something is wrong - put method shouldnt be called by backup node");
		}
		
		try {
			//acquire write lock, the thread will be blocked until the lock can be acquired
			this.readWriteLock.writeLock().lock();
			this.myMap.put(key, value);
			//log.info("Primary map content: " + this.getMyMap().toString());
			
			//start to update MyMap for all backup nodes
			for(String backupSocket: this.backupSockets) {
				// establish RPC from primary to backup socket
				String backupHost = backupSocket.split(":")[0];
				int backupPort = Integer.parseInt(backupSocket.split(":")[1]);
				TSocket tSocket = new TSocket(backupHost, backupPort);
	            TTransport tTransport = new TFramedTransport(tSocket);
	            tTransport.open();
	            TProtocol tProtocol = new TBinaryProtocol(tTransport);
	            
	            // retrieve backup client
	            KeyValueService.Client backupClient = new KeyValueService.Client(tProtocol);
	            
	            // start to synchronize the backup node with primary node
	            log.info("Start updating data in backup node" + backupHost + ":" + backupPort + " because primary node has new changes.");
	            backupClient.syncWithPrimary(key, value);
	            log.info("Finished updating data in backup node" + backupHost + ":" + backupPort);
	           tTransport.close();
			}
		}catch(Exception e) {
			log.error("PUT METHOD ERROR");
			e.printStackTrace();
		}finally {
			this.readWriteLock.writeLock().unlock();
		}
	}
	
	public void syncWithPrimary(String key, String value) throws org.apache.thrift.TException {
		if(this.isPrimary()) {
			log.error("Something is wrong - syncWithPrimary method shouldnt be called by primary node");
			throw new org.apache.thrift.TException("Something is wrong - syncWithPrimary method shouldnt be called by primary node");
		}
		
		try {
			//acquire write lock, the thread will be blocked until the lock can be acquired
			log.info("Backup node is syncing with primary node");
			this.readWriteLock.writeLock().lock();
			this.myMap.put(key, value);
			//log.info("Backup map content: " + this.getMyMap().toString());
		}catch(Exception e) {
			log.error("SYNC METHOD ERROR");
			e.printStackTrace();
		}finally {
			this.readWriteLock.writeLock().unlock();
		}
	}
	
	public void replicateData(Map<String, String> primaryData) throws org.apache.thrift.TException{
		if(this.isPrimary()) {
			log.error("Something is wrong - replicateData method shouldnt be called by primary node");
			throw new org.apache.thrift.TException("Something is wrong - replicateData method shouldnt be called by primary node");
		}
		try {
			readWriteLock.writeLock().lock();
			myMap.putAll(primaryData);
		}catch(Exception e) {
			log.error("Failed to replicate data from primary node to backup node:" + this.host + ":" + this.port);
			e.printStackTrace();
		}finally {
			readWriteLock.writeLock().unlock();
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
	            log.info("Start replicating data from primary node to backup node" + this.host + ":" + this.port);
	            this.replicateData(primaryClient.getMyMap());
	            log.info("Finished replicating data from primary node to backup node" + this.host + ":" + this.port);
	            tTransport.close();
			}		
		}catch(InterruptedException e) {
			e.printStackTrace();
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
}
