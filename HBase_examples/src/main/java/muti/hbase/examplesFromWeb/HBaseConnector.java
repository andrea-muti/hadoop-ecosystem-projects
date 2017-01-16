package muti.hbase.examplesFromWeb;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class HBaseConnector {

	static final Logger logger = Logger.getLogger(HBaseConnector.class);

	static final String HBASE_IP = "192.168.177.101";
	static final String ZK_IP = "192.168.177.101";
	
	static final String TABLE_NAME = "myLittleHBaseTable";

	public static void main(String[] args) throws IOException {

		BasicConfigurator.configure();
		Logger.getRootLogger().setLevel(Level.toLevel("INFO"));

		System.setProperty("hadoop.home.dir", "C:\\Users\\Andrea\\hadoop-common-2.2.0-bin-master");

		Configuration config = HBaseConfiguration.create();
		config.set("hbase.master", HBASE_IP+":160010");
		config.set("hbase.zookeeper.quorum", ZK_IP);

		Connection conn = ConnectionFactory.createConnection(config);
		logger.info("connected to HBase");

		Admin admin = conn.getAdmin();

		HTableDescriptor htabledesc = new HTableDescriptor(TableName.valueOf(TABLE_NAME));

		Table hTable = null;
		if (!admin.tableExists(htabledesc.getTableName())) {
			logger.info("table '"+TABLE_NAME+"' does not exist - creating it...");
			htabledesc.addFamily(new HColumnDescriptor("myLittleFamily").setCompressionType(Algorithm.NONE));
			admin.createTable(htabledesc);
			logger.info("table '"+TABLE_NAME+"' successfully created");
		}
		
		hTable = conn.getTable(TableName.valueOf(TABLE_NAME));
		logger.info("obtained table descriptor to table "+TABLE_NAME);
		
		// To add to a row, use Put. A Put constructor takes the name of the row you want to insert into as a byte array. 
		// In HBase, the Bytes class has utility for converting all kinds of Java types to byte arrays. 
		// In the below, we are converting the String "myLittleRow" into a byte array to use as a row key for our update. 
		// Once you have a Put instance, you can adorn it by setting the names of columns you want
		// to update on the row, the timestamp to use in your update, etc.
		// If no timestamp, the server applies current time to the edits.
		Put p = new Put(Bytes.toBytes("myLittleRow"));

		// To set the value you'd like to update in the row 'myLittleRow', specify the column family, 
		// column qualifier, and value of the table cell you'd like to update. 
		// The column family must already exist in your table schema. The qualifier can be anything.
		// All must be specified as byte arrays as hbase is all about byte arrays. 
		// Make sure that the table 'myLittleHBaseTable' was created with a family 'myLittleFamily'.
		p.addColumn(Bytes.toBytes("myLittleFamily"), Bytes.toBytes("someQualifier"), Bytes.toBytes("Some Value"));

		// Once you've adorned your Put instance with all the updates you want to make, to commit it do the following
		// (The HTable#put method takes the Put instance you've been building and pushes the changes you made into hbase)
		hTable.put(p);

		// Now, to retrieve the data we just wrote. The values that come back are Result instances. 
		// Generally, a Result is an object that will package up the hbase return into the form you find most palatable.
		Get g = new Get(Bytes.toBytes("myLittleRow"));
		Result r = hTable.get(g);
		byte[] value = r.getValue(Bytes.toBytes("myLittleFamily"), Bytes.toBytes("someQualifier"));

		// If we convert the value bytes, we should get back 'Some Value', the value we inserted at this location.
		String valueStr = Bytes.toString(value);
		System.out.println("GET: " + valueStr);

		// Sometimes, you won't know the row you're looking for. In this case, you use a Scanner. 
		// This will give you cursor-like interface to the contents of the table. 
		// To set up a Scanner, do like you did above making a Put and a Get, create a Scan. 
		// Adorn it with column names, etc.
		Scan s = new Scan();
		s.addColumn(Bytes.toBytes("myLittleFamily"), Bytes.toBytes("someQualifier"));

		ResultScanner scanner = hTable.getScanner(s);
		try {
			// Scanners return Result instances.
			for (Result rr : scanner) {
				 System.out.println("Found row: " + rr);
			}

		} finally {
			// Make sure you close your scanners when you are done!
			// Thats why we have it inside a try/finally clause
			scanner.close();
		}
		
//		admin.disableTable(htabledesc.getTableName());
//		admin.deleteTable(htabledesc.getTableName());
		
		hTable.close();
		admin.close();
		conn.close();
	}
}