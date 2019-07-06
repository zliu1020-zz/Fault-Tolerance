import java.util.*;
import java.util.concurrent.*;

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

	public KeyValueHandler(String host, int port, CuratorFramework curClient, String zkNode) {
		this.host = host;
		this.port = port;
		this.curClient = curClient;
		this.zkNode = zkNode;
		myMap = new ConcurrentHashMap<String, String>();
		
		BasicConfigurator.configure();
		log = Logger.getLogger(KeyValueHandler.class.getName());
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
	
	@Override
	synchronized public void process(WatchedEvent event) {
		System.out.println("Zookeeper event caught: " + event.toString());
		try {
			List<String> childNodes = fetchZookeeperChildNodes();
			System.out.println("The number of active child nodes: " + childNodes.size());
			System.out.println("The list of active child nodes:" + childNodes.toString());
		}catch(InterruptedException e) {
			e.printStackTrace();
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
}
