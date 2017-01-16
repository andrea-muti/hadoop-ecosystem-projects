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
 * InsertData
 * 
 * <p>Example showing how to insert a row in a (previously existing) HBase table.
 * The table schema associated to the row can be defined at the moment of row insertion.
 * In particular the row is associated to two column families, 
 * the former with two columns, and the latter with three columns.
 * Values for each column of the row are populated with example data.
 *  
 * @author Andrea Muti
 * created: 15 gen 2017
 *
 */

public class InsertData {

	static final String HBASE_IP = "192.168.177.101";
	static final String HBASE_PORT = "160010";
	static final String ZK_IP = "192.168.177.101";

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
		if (!hAdmin.tableExists((TableName.valueOf("employee")))) {
			System.err.println("a table called 'employee' must exist in the DB in order to run this example");
		}
		else {
			
			// Instantiating HTable class
			Table hTable = conn.getTable(TableName.valueOf("employee"));

			// Instantiating Put class
			// accepts a row name. (this is basically the row key)
			Put p = new Put(Bytes.toBytes("AM-key"));

			// adding values using add() method
			// accepts column family name, qualifier/row name ,value
			p.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("name"), Bytes.toBytes("Andrea Muti"));
			p.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("city"), Bytes.toBytes("Aprilia"));
			p.addColumn(Bytes.toBytes("professional"),Bytes.toBytes("company"), Bytes.toBytes("Capgemini SpA Italia"));
			p.addColumn(Bytes.toBytes("professional"),Bytes.toBytes("designation"), Bytes.toBytes("Analyst Consultant"));
			p.addColumn(Bytes.toBytes("professional"),Bytes.toBytes("salary"), Bytes.toBytes("21500"));

			// Saving the put Instance to the HTable.
			hTable.put(p);
			System.out.println("data inserted");

			// closing HTable
			hTable.close();
		}

		// Close Admin
		hAdmin.close();

		// Close Connection
		conn.close();
	}
}
