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

import com.google.common.util.concurrent.Striped;

public class KeyValueHandler implements KeyValueService.Iface, CuratorWatcher{
	private Map<String, String> myMap;
	private CuratorFramework curClient;
	private String zkNode;
	private String host;
	private int port;
	private static Logger log;
	private String primarySocket;
	private List<String> backupSockets;
	private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private ConcurrentLinkedQueue<KeyValueService.Client> clientPool = new ConcurrentLinkedQueue<KeyValueService.Client>();
    
    private Striped<Semaphore> striped = Striped.semaphore(64,1);

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
		Semaphore mutex = striped.get(key);
		try {
			//acquire read lock, the thread will be blocked until the lock can be acquired
			log.info("GET method called");
            mutex.acquire();
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
            mutex.release();
		}
		
		// if everything goes well the execution flow should never reach here 
		throw new TException("get operation failed");	
	}

	public void put(String key, String value) throws org.apache.thrift.TException {
		if(!this.isPrimary()) {
			log.error("Something is wrong - put method shouldnt be called by backup node");
			throw new org.apache.thrift.TException("Something is wrong - put method shouldnt be called by backup node");
		}
		Semaphore mutex = striped.get(key);
		try {
			//acquire write lock, the thread will be blocked until the lock can be acquired
			log.info("PUT method called");

            mutex.acquire();
			this.myMap.put(key, value);

			//start to update MyMap for backup node
			for(String backupSocket: this.backupSockets) {
                KeyValueService.Client myClient = null;
                while(myClient == null){
                    myClient = this.clientPool.poll();
                }
                myClient.syncWithPrimary(key, value);
                this.clientPool.add(myClient);
			}
		}catch(Exception e) {
			log.error("PUT METHOD ERROR");
			e.printStackTrace();
		}finally {
            mutex.release();
		}
	}
	
	public void syncWithPrimary(String key, String value) throws org.apache.thrift.TException {
		if(this.isPrimary()) {
			log.error("Something is wrong - syncWithPrimary method shouldnt be called by primary node");
			throw new org.apache.thrift.TException("Something is wrong - syncWithPrimary method shouldnt be called by primary node");
		}
		Semaphore mutex = striped.get(key);
		try {
			//acquire write lock, the thread will be blocked until the lock can be acquired
			log.info("Backup node is syncing with primary node");

            mutex.acquire();
			this.myMap.put(key, value);
		}catch(Exception e) {
			log.error("SYNC METHOD ERROR");
			e.printStackTrace();
		}finally {
            mutex.release();
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
    
    private ConcurrentLinkedQueue<KeyValueService.Client> populateClientPool() throws org.apache.thrift.TException{
        ConcurrentLinkedQueue<KeyValueService.Client> queue = new ConcurrentLinkedQueue<KeyValueService.Client>();
        
        for(int i = 0; i < 64; i++){
            // establish RPC from primary to backup socket
            String backupSocket = this.backupSockets.get(0);
            
            String backupHost = backupSocket.split(":")[0];
            int backupPort = Integer.parseInt(backupSocket.split(":")[1]);
            TSocket tSocket = new TSocket(backupHost, backupPort);
	        TTransport tTransport = new TFramedTransport(tSocket);
	        tTransport.open();
	        TProtocol tProtocol = new TBinaryProtocol(tTransport);
	            
	        // retrieve backup client
	        KeyValueService.Client myClient = new KeyValueService.Client(tProtocol);
            
            // add to the queue
            queue.add(myClient);
        }
        
        return queue;
    }
	
	@Override
	synchronized public void process(WatchedEvent event) {
		log.info("Zookeeper event caught: " + event.toString());
		if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
			this.backupSockets.clear();
			log.info("NodeChildrenChanged event happens. Backup list resets.");
		}
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
            
            // if the current node is primary node and there exists backup node, start to prepare backup clients
            if(this.isPrimary() && this.backupSockets.size() > 0){
                this.clientPool = populateClientPool();
                log.info("Done populating client pool: " + this.clientPool.toString());
            }
		}catch(InterruptedException e) {
			e.printStackTrace();
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
}
