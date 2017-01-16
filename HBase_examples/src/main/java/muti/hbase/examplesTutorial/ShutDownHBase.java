package muti.hbase.examplesTutorial;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * ShutDownHBase
 * 
 * <p>Example showing how to programmatically shutdown the HBase cluster.
 *    It's assumed that the HBase cluster is up and running when the code is executed.
 *  
 * @author Andrea Muti
 * created: 15 gen 2017
 *
 */

public class ShutDownHBase {

	static final String HBASE_IP = "192.168.177.101";
	static final String HBASE_PORT = "160010";
	static final String ZK_IP = "192.168.177.101";

	public static void main(String args[]) throws IOException {

		System.setProperty("hadoop.home.dir", "C:\\Users\\Andrea\\hadoop-common-2.2.0-bin-master");

		// Configure logging
		BasicConfigurator.configure();
		Logger.getRootLogger().setLevel(Level.toLevel("WARN"));

		// Instantiating configuration class
		Configuration conf = HBaseConfiguration.create();
		conf.clear();
		conf.set("hbase.master", HBASE_IP+":"+HBASE_PORT);
		conf.set("hbase.zookeeper.quorum", ZK_IP); 

		try {
			
			// Instantiating Connection class
			Connection conn = ConnectionFactory.createConnection(conf);

			// Instantiating Admin class
			Admin hAdmin = conn.getAdmin();

			
			// ATTENTION : if HBase is running its own instance of ZooKeeper, 
			// then the ZooKeeper process, namely HQuorumPeer, will NOT be shut down by executing the shutdown() method
			
			// Shutting down HBase
			System.out.print("Shutting down HBase: ");
			hAdmin.shutdown();
			System.out.println("DONE");
			
			// Closing Adming
			hAdmin.close();
			
			// Closing Connection
			conn.close();
		}
		catch (Exception e) {
			System.err.println("error while creating connection to HBase: "+e.getMessage());
		}
	}
}