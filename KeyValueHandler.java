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
    //private boolean isPrimary;
	private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private volatile ConcurrentLinkedQueue<KeyValueService.Client> clientPool = new ConcurrentLinkedQueue<KeyValueService.Client>();
    
    private Striped<Semaphore> striped = Striped.semaphore(64,1);

	public KeyValueHandler(String host, int port, CuratorFramework curClient, String zkNode) {
		this.host = host;
		this.port = port;
		this.curClient = curClient;
		this.zkNode = zkNode;
		myMap = new ConcurrentHashMap<String, String>();
		
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
		Semaphore mutex = striped.get(key);
		try {
			//acquire read lock, the thread will be blocked until the lock can be acquired
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
			// release the read lock no matter what
            mutex.release();
		}
		
		// if everything goes well the execution flow should never reach here 
		throw new TException("get operation failed");	
	}

	public void put(String key, String value) throws org.apache.thrift.TException {
		Semaphore mutex = striped.get(key);
		try {
			//acquire write lock, the thread will be blocked until the lock can be acquired
            mutex.acquire();
			this.myMap.put(key, value);
          
            if(this.clientPool != null){
                //start to update MyMap for backup node
			    KeyValueService.Client myClient = null;
                while(myClient == null){
                    myClient = this.clientPool.poll();
                }
            
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
		Semaphore mutex = striped.get(key);
        try {
            mutex.acquire();
			this.myMap.put(key, value);
		}catch(Exception e) {
			log.error("SYNC METHOD ERROR");
			log.error(e.getMessage());
		}finally{
            mutex.release();
        }
	}
	
	public void replicateData(Map<String, String> primaryData) throws org.apache.thrift.TException{
		try {
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
        for(int i = 0; i < 32; i++){
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
		try {
			// fetch all children
			List<String> childNodes = fetchZookeeperChildNodes();
			log.info("The number of active child nodes: " + childNodes.size());
			log.info("The list of active child nodes:" + childNodes.toString());
			
			// memorize primary socket (it's the first one in the sorted list)
			String primarySocket = new String(this.curClient.getData().forPath(this.zkNode + "/" + childNodes.get(0)));
            String backupSocket = null;
            if(childNodes.size() > 1){
                backupSocket = new String(this.curClient.getData().forPath(this.zkNode + "/" + childNodes.get(1)));    
            }
			log.info("Current node is" + this.host + ":" + this.port + ". isPrimary: " + this.checkIfPrimary(primarySocket));
            
            if(childNodes.size() == 3){
                log.error("********WE'RE FUCKED***********");
                log.error("1st = " + new String(this.curClient.getData().forPath(this.zkNode + "/" + childNodes.get(0))));
                log.error("2nd = " + new String(this.curClient.getData().forPath(this.zkNode + "/" + childNodes.get(1))));
                log.error("3rd = " + new String(this.curClient.getData().forPath(this.zkNode + "/" + childNodes.get(2))));
            }
            
            // if the current node is primary node and there exists backup node, start to prepare backup clients
            if(this.checkIfPrimary(primarySocket) && backupSocket != null){
                this.clientPool = populateClientPool(backupSocket);
                log.info("Done populating client pool, size: " + this.clientPool.size());
                
                KeyValueService.Client myClient = null;
                while(myClient == null){
                    myClient = this.clientPool.poll();
                }
                try {
			         readWriteLock.writeLock().lock();
			         myClient.replicateData(this.myMap);
		        }catch(Exception e) {
			         log.error("Failed to replicate data from primary node to backup node:" + this.host + ":" + this.port);
			         log.error(e.getMessage());
		        }finally {
			         readWriteLock.writeLock().unlock();
		        }
                if(myClient != null){
                    this.clientPool.add(myClient);    
                }
            }
            
            // if the current node is primary node and the backend node is down, purge the backup client queue
             if(this.checkIfPrimary(primarySocket) && backupSocket == null){
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
