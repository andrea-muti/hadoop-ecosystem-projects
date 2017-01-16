package muti.hbase.examplesTutorial;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * UpdateData
 * 
 * <p>Example showing how to update data in row of a (previously existing) HBase table.
 *  
 * @author Andrea Muti
 * created: 15 gen 2017
 *
 */

public class UpdateData {

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

		// check whether or not the table already exists
		if (!hAdmin.tableExists((TableName.valueOf(TABLE_NAME)))) {
			System.err.println("error: a table called '"+TABLE_NAME+"' must exist in the DB in order to run this example");
		}
		else{
			
			// Instantiating HTable class
			Table hTable = conn.getTable(TableName.valueOf(TABLE_NAME));

			// Instantiating Put class
			// accepts a row name
			Put p = new Put(Bytes.toBytes("AM-key"));
		
			// Updating a cell value
			p.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("city"),Bytes.toBytes("Roma"));

			// Saving the put Instance to the HTable.
			hTable.put(p);
			System.out.println("data Updated");

			// closing HTable
			hTable.close();
		}

		// Close Admin
		hAdmin.close();

		// Close Connection
		conn.close();
	}
}
