package muti.zookeeper.examples;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * ZooKeeperClient
 * 
 * @author Andrea Muti
 * created: 11 dic 2016
 *
 */

public class ZooKeeperClient {

	// declare zookeeper instance to access ZooKeeper ensemble
	private ZooKeeper zoo;
	final CountDownLatch connectedSignal = new CountDownLatch(1);

	private int counterConn;

	/**
	 * public constructor
	 */
	public ZooKeeperClient(){
		configureLogging();
		this.zoo = null;
		this.counterConn = 0;
	}

	private void configureLogging(){
		org.apache.log4j.BasicConfigurator.configure();
	}

	// Method to connect zookeeper ensemble.
	public ZooKeeper connect(String host) throws IOException,InterruptedException {
		int sessionTimeout = 5000;		
		this.zoo = new ZooKeeper(host, sessionTimeout, new Watcher() {
			public void process(WatchedEvent we) {
				if (we.getState() == KeeperState.SyncConnected) {
					connectedSignal.countDown();
				}
			}
		});
		connectedSignal.await();
		this.counterConn++;
		return zoo;
	}

	public int getCounter(){
		return this.counterConn;
	}

	// Method to create znode in zookeeper ensemble
	public boolean createZnode(String path, byte[] data) throws KeeperException, InterruptedException {
		try{
			this.zoo.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			return true;
		}
		catch(Exception e){
			return false;
		}
	}

	// Method to check existence of znode and its status, if znode is available.
	public Stat znodeExists(String path) throws KeeperException, InterruptedException {
		return zoo.exists(path, true);
	}

	public byte[] getDataFromNode(String path) throws KeeperException, InterruptedException{
		Stat stat = znodeExists(path);

		if (stat != null) {
			byte[] b = this.zoo.getData(path, new Watcher() {

				public void process(WatchedEvent we) {
					try {
						zoo.getData(path, false, null);
						connectedSignal.countDown();
					} catch(Exception ex) {
						System.out.println(ex.getMessage());
						connectedSignal.countDown();
					}
				}
			}, null);
			connectedSignal.await();
			return b;
		} else { // the znode does not exists
			return null;
		}
	}

	public List <String> getChildren(String path) throws KeeperException, InterruptedException {

		Stat stat = znodeExists(path);  // Stat checks the path

		if(stat!= null) {
			//“getChildren” method- get all the children of znode. It has two args, path and watcher
			List <String> children = zoo.getChildren(path, false);
			return children;
		} else { // znode does not exists
			return null;
		}
	}


	// Method to update the data in a znode. Similar to getData but without watcher.
	public void update(String path, byte[] data) throws KeeperException, InterruptedException {
		this.zoo.setData(path, data, this.zoo.exists(path,true).getVersion());
	}

	// Method to delete an existing znode assuming it has no children.
	public void deleteNode(String path) throws KeeperException, InterruptedException {
		this.zoo.delete(path, this.zoo.exists(path,true).getVersion());
	}

	// Method to delete an existing znode assuming it has no children.
	public void deleteNodeWithChildren(String path) throws KeeperException, InterruptedException {
		// get children of the node
		List<String> children = this.zoo.getChildren(path, false);
		
		String childPath;
		// delete children of the node
		for(String child : children) {
			childPath = path+"/"+child;
			this.zoo.delete(childPath, this.zoo.exists(childPath,true).getVersion());
		}
		
		// finally delete the node itselft
		this.zoo.delete(path, this.zoo.exists(path,true).getVersion());
	}


	// Method to disconnect from zookeeper server
	public void close() throws InterruptedException {
		this.zoo.close();
	}
}
