package muti.hbase.examplesTutorial;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * RetrieveData
 * 
 * <p>Example showing how to scan a (previously existing) HBase table.
 * The Scan operation is setup in order to retrieve only given columns for each row of the table.
 * 
 * @author Andrea Muti
 * created: 15 gen 2017
 *
 */

public class ScanTable {

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

			// Instantiating the Scan class
			Scan scan = new Scan();

			// Scanning the required columns
			scan.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("name"));
			scan.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("city"));

			// Getting the scan result
			ResultScanner scanner = hTable.getScanner(scan);

			// Reading values from scan result
			for (Result result = scanner.next(); result != null; result = scanner.next()){
				System.out.println("Found row : " + result);
			}

			//closing the scanner
			scanner.close();

			// closing HTable
			hTable.close();
		}

		// Close Admin
		hAdmin.close();

		// Close Connection
		conn.close();
	}
}