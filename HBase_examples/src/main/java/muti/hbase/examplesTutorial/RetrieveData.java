package muti.hbase.examplesTutorial;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * RetrieveData
 * 
 * <p>Example showing how to retrieve a row from a (previously existing) HBase table, given the row key.
 * Some of the attributes of the retrieved row are then printed on console.
 *  
 * @author Andrea Muti
 * created: 15 gen 2017
 *
 */

public class RetrieveData {

	static final String HBASE_IP = "192.168.177.101";
	static final String HBASE_PORT = "160010";
	static final String ZK_IP = "192.168.177.101";

	public static void main(String[] args) throws Exception {

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
		if (!hAdmin.tableExists((TableName.valueOf("employee")))) {
			System.err.println("a table called 'employee' must exist in the DB in order to run this example");
		}
		else {

			// Instantiating HTable class
			Table hTable = conn.getTable(TableName.valueOf("employee"));

			// Instantiating Get class
			Get g = new Get(Bytes.toBytes("AM-key"));

			// Reading the data
			Result result = hTable.get(g);

			// Reading values from Result class object
			byte [] value = result.getValue(Bytes.toBytes("personal"),Bytes.toBytes("name"));

			byte [] value1 = result.getValue(Bytes.toBytes("personal"),Bytes.toBytes("city"));

			// Printing the values
			String name = Bytes.toString(value);
			String city = Bytes.toString(value1);

			System.out.println("name: " + name + " - city: " + city);

			// closing HTable
			hTable.close();

		}

		// Close Admin
		hAdmin.close();

		// Close Connection
		conn.close();
	}
}