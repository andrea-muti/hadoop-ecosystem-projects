package muti.hbase.examplesTutorial;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * DeleteTable
 * 
 * <p>Example showing how to remove a (previously existing) HBase table.
 *  
 * @author Andrea Muti
 * created: 15 gen 2017
 *
 */

public class DeleteTable {

	static final String HBASE_IP = "192.168.177.101";
	static final String HBASE_PORT = "160010";
	static final String ZK_IP = "192.168.177.101";

	static final String TABLE_NAME = "employee";

	public static void main(String[] args) throws IOException {

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

		// Instantiating table descriptor class
		HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));

		// Check whether the table already exists or not
		if (hAdmin.tableExists(tableDescriptor.getTableName())) {

			// disabling table named emp
			hAdmin.disableTable(TableName.valueOf(TABLE_NAME));

			// Deleting emp
			hAdmin.deleteTable(TableName.valueOf(TABLE_NAME));
			System.out.println("Table deleted");

		}
		else {
			System.err.println("error: table '"+TABLE_NAME+"' does not exist - cannot delete");
		}

		// Close Admin
		hAdmin.close();

		// Close Connection
		conn.close();

	}
}