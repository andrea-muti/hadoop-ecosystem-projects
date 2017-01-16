package muti.hbase.examplesTutorial;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * ListTables
 * 
 * <p>Example showing how to list (i.e. print on console) the names 
 *    of all the tables stored in the HBase database. 
 *    Furthermore, for each of the retrieved tables, its column family names are listed.
 *  
 * @author Andrea Muti
 * created: 15 gen 2017
 *
 */

public class ListTables {

	static final String HBASE_IP = "192.168.177.101";
	static final String HBASE_PORT = "160010";
	static final String ZK_IP = "192.168.177.101";

	public static void main(String args[]) throws MasterNotRunningException, IOException {

		System.setProperty("hadoop.home.dir", "C:\\Users\\Andrea\\hadoop-common-2.2.0-bin-master");

		// Configure logging
		BasicConfigurator.configure();
		Logger.getRootLogger().setLevel(Level.toLevel("WARN"));

		// Instantiating configuration class
		Configuration conf = HBaseConfiguration.create();
		conf.clear();
		conf.set("hbase.master", HBASE_IP+":"+HBASE_PORT);
		conf.set("hbase.zookeeper.quorum", ZK_IP); 

		// Instantiating Connection class
		Connection conn = ConnectionFactory.createConnection(conf);

		// Instantiating Admin class
		Admin hAdmin = conn.getAdmin();


		// Getting all the list of tables using HBaseAdmin object
		HTableDescriptor[] tableDescriptors = hAdmin.listTables();

		// printing all the table names.
		System.out.println("--- All Table Names ---");
		for(int i=0; i < tableDescriptors.length;i++){
			System.out.println(" - "+tableDescriptors[i].getNameAsString());
		}

		// for each table, printing all its column family names.
		System.out.println("\n--- All Table Names With Column Families ---");
		for(int i=0; i < tableDescriptors.length;i++){
			System.out.println("\n - "+tableDescriptors[i].getNameAsString());
			
			HColumnDescriptor[] columnFamilies = tableDescriptors[i].getColumnFamilies();
			for (int j=0; j < columnFamilies.length; j++) {
				System.out.println("\t - "+columnFamilies[j].getNameAsString());
			}
		}

		// Close Admin
		hAdmin.close();

		// Close Connection
		conn.close();
	}

}
