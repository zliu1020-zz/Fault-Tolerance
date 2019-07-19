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
import java.util.concurrent.atomic.AtomicBoolean;

public class KeyValueHandler implements KeyValueService.Iface, CuratorWatcher{
	private Map<String, String> myMap;
	private CuratorFramework curClient;
	private String zkNode;
	private String host;
	private int port;
	private static Logger log;
    private AtomicBoolean isPrimary;
    private AtomicBoolean hasThreeNode;
    private AtomicBoolean isBackupWIP;
	private final ReentrantLock lock;
    private volatile ConcurrentLinkedQueue<KeyValueService.Client> clientPool;
    
    private Striped<Semaphore> striped = Striped.semaphore(64,1);

	public KeyValueHandler(String host, int port, CuratorFramework curClient, String zkNode) {
		this.host = host;
		this.port = port;
		this.curClient = curClient;
		this.zkNode = zkNode;
        this.hasThreeNode = new AtomicBoolean();
        this.isPrimary = new AtomicBoolean();
        this.isBackupWIP = new AtomicBoolean();
		this.myMap = new ConcurrentHashMap<String, String>();
        this.lock = new ReentrantLock();
        this.clientPool = null;
		
		//set up log4j
		BasicConfigurator.configure();
		log = Logger.getLogger(KeyValueHandler.class.getName());
		
		// bind watcher
	    try {
	        this.curClient.getChildren().usingWatcher(this).forPath(this.zkNode);
	    } catch (Exception e) {
	        log.error(e.getMessage());
	    }
	}

	public String get(String key) throws org.apache.thrift.TException {
        if(!this.isPrimary.get()){
            log.error("***REJECT GET REQUEST FROM BACKUP NODE**");
            throw new TException("***REJECT GET REQUEST FROM BACKUP NODE***");
        }
        
		Semaphore mutex = striped.get(key);
		try {
            mutex.acquire();
			String ret = myMap.get(key);
			if (ret == null)
				return "";
			else
				return ret;
		}catch(Exception e) {
			log.error("Error reading from MyMap.");
			log.error(e.getMessage());
		}finally {
            mutex.release();
		}
		
		// if everything goes well the execution flow should never reach here 
		throw new TException("get operation failed");	
	}

	public void put(String key, String value) throws org.apache.thrift.TException {
        if(!this.isPrimary.get()){
            log.error("***REJECT PUT REQUEST FROM BACKUP NODE***");
            throw new TException("***REJECT PUT REQUEST FROM BACKUP NODE***");
        }
        
		Semaphore mutex = striped.get(key);
		try {
            mutex.acquire();
            while(this.lock.isLocked() && this.isBackupWIP.get()){
                // waiting for the primary replication to finish
            }
            
            this.myMap.put(key, value);
          
            if(this.clientPool != null){
			    KeyValueService.Client myClient = null;

                while(this.clientPool.isEmpty()){
                    //wait for the queue to be refilled
                }
                myClient = this.clientPool.poll();
                myClient.syncWithPrimary(key, value);
                try{
                    this.clientPool.add(myClient);    
                }catch(java.lang.NullPointerException e){
                    log.error(e.getMessage());
                }
            }
		}catch(Exception e) {
			log.error("PUT METHOD ERROR");
			log.error(e.getMessage());
		}finally {
            mutex.release();
		}
	}
	
	public void syncWithPrimary(String key, String value) throws org.apache.thrift.TException {
        try {
			this.myMap.put(key, value);
		}catch(Exception e) {
			log.error("SYNC METHOD ERROR");
			log.error(e.getMessage());
		}
	}
	
	public void replicateData(Map<String, String> primaryData) throws org.apache.thrift.TException{
		try {
			 myMap.clear();
             myMap.putAll(primaryData);
		}catch(Exception e) {
			log.error("Failed to replicate data from primary node to backup node:" + this.host + ":" + this.port);
			log.error(e.getMessage());
		}
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
	
	private boolean checkIfPrimary(String primarySocket) {
		if(primarySocket == null) {
			log.error("This shouldn't happen at all");
			return false;
		}
		String primaryHost = primarySocket.split(":")[0];
		int primaryPort = Integer.parseInt(primarySocket.split(":")[1]);
		
		return this.host.equals(primaryHost) && this.port == primaryPort;
	}
    
    private ConcurrentLinkedQueue<KeyValueService.Client> populateClientPool(String backupSocket) throws org.apache.thrift.TException{
        ConcurrentLinkedQueue<KeyValueService.Client> queue = new ConcurrentLinkedQueue<KeyValueService.Client>();    
        for(int i = 0; i < 15; i++){
            String backupHost = backupSocket.split(":")[0];
            int backupPort = Integer.parseInt(backupSocket.split(":")[1]);
            TSocket tSocket = new TSocket(backupHost, backupPort);
	        TTransport tTransport = new TFramedTransport(tSocket);
	        tTransport.open();
	        TProtocol tProtocol = new TBinaryProtocol(tTransport);
	        KeyValueService.Client myClient = new KeyValueService.Client(tProtocol);
            
            queue.add(myClient);
        }
        
        return queue;
    }
    
    private void init(String backupSocket) throws Exception{
        String backupHost = backupSocket.split(":")[0];
        int backupPort = Integer.parseInt(backupSocket.split(":")[1]);
        TSocket tSocket = new TSocket(backupHost, backupPort);
	    TTransport tTransport = new TFramedTransport(tSocket);
        tTransport.open();
	    TProtocol tProtocol = new TBinaryProtocol(tTransport);
	    KeyValueService.Client myClient = new KeyValueService.Client(tProtocol);
        try {
			 this.lock.lock();
             this.isBackupWIP.set(true);
             myClient.replicateData(this.myMap);
             this.clientPool = populateClientPool(backupSocket);
             log.info("Done populating client pool, size: " + this.clientPool.size());
        }catch(Exception e) {
			 log.error("Failed to replicate data from primary node to backup node:" + this.host + ":" + this.port);
			 log.error(e.getMessage());
        }finally {
			 this.lock.unlock();
             this.isBackupWIP.set(false);
             log.info("Done replicating data from primary to backup");
        }    
    }
	
	@Override
	synchronized public void process(WatchedEvent event) {
		log.info("Zookeeper event caught: " + event.toString());
		try {
			// fetch all children
			List<String> childNodes = fetchZookeeperChildNodes();
			log.info("The number of active child nodes: " + childNodes.size());
			log.info("The list of active child nodes:" + childNodes.toString());
            
            int primaryIdx = childNodes.size() > 2 ? childNodes.size() - 2 : 0;
            int backupIdx = childNodes.size() > 2 ? childNodes.size() - 1 : 1;
            boolean threeNode = childNodes.size() > 2 ? true : false;
            
            this.hasThreeNode.set(threeNode);
            String primarySocket = new String(this.curClient.getData().forPath(this.zkNode + "/" + childNodes.get(primaryIdx)));
			String backupSocket = childNodes.size() > 1 ? new String(this.curClient.getData().forPath(this.zkNode + "/" + childNodes.get(backupIdx))) : null;
            
//            String primarySocket = null;
//            String backupSocket = null;
//            String brokenSocket = null;
//            if(childNodes.size() > 2){
//                this.hasThreeNode.set(true);
//                primarySocket = new String(this.curClient.getData().forPath(this.zkNode + "/" + childNodes.get(childNodes.size()-2)));
//                backupSocket = new String(this.curClient.getData().forPath(this.zkNode + "/" + childNodes.get(childNodes.size()-1)));
//                log.warn("******** THREE NODES FOUND ***********");
//            }else{
//                this.hasThreeNode.set(false);
//                primarySocket = new String(this.curClient.getData().forPath(this.zkNode + "/" + childNodes.get(0)));
//                if(childNodes.size() > 1){
//                    backupSocket = new String(this.curClient.getData().forPath(this.zkNode + "/" + childNodes.get(1)));    
//                }
//            }
            
            this.isPrimary.set(this.checkIfPrimary(primarySocket));	
			log.info("Current node is" + this.host + ":" + this.port + ". isPrimary: " + this.isPrimary.get());
            
            // if the current node is primary node and there exists backup node, start to prepare backup clients
            if(this.isPrimary.get() && backupSocket != null){
                this.init(backupSocket);
            }
        
            // if the current node is primary node and the backend node is down, purge the backup client queue
             if(this.isPrimary.get() && backupSocket == null){
                this.clientPool = null;
                log.info("Done populating client pool, size: 0"); 
            }
		}catch(InterruptedException e) {
			log.error(e.getMessage());
		}catch(Exception e) {
			log.error(e.getMessage());
		}
	}
}