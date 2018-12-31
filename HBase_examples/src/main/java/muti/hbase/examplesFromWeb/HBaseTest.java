package muti.hbase.examplesFromWeb;


import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.mortbay.log.Log;

public class HBaseTest {

	static final Logger logger = Logger.getLogger(HBaseConnector.class);

	static final String HBASE_IP = "192.168.177.101";
	static final String ZK_IP = "192.168.177.101";

	private static Configuration conf = null;
	private static Connection conn = null;
	private static Admin admin = null;

	/**
	 * Initialization
	 */
	static {

		BasicConfigurator.configure();
		Logger.getRootLogger().setLevel(Level.toLevel("INFO"));

		System.setProperty("hadoop.home.dir", "C:\\Users\\Andrea\\hadoop-common-2.2.0-bin-master");

		conf = HBaseConfiguration.create();
		conf.clear();
		conf.set("hbase.master", HBASE_IP+":160010");
		conf.set("hbase.zookeeper.quorum", ZK_IP);

		try {
			conn = ConnectionFactory.createConnection(conf);
			admin = conn.getAdmin();
			Log.info("CONNECTED");
		} 
		catch (IOException e) {
			logger.error("ERROR while creating a connection to HBase: "+e.getMessage());
			System.exit(-1);
		}
	}

	/**
	 * Create a table
	 */
	public static void createTable(String tableName, String[] familys) throws Exception {
		HTableDescriptor htabledesc = new HTableDescriptor(TableName.valueOf(tableName));
		if (admin.tableExists(htabledesc.getTableName())) {
			logger.info("the table '"+tableName+"' already exists!");
		}
		else {	
			for (int i = 0; i < familys.length; i++) {
				htabledesc.addFamily(new HColumnDescriptor(familys[i]));
			}
			admin.createTable(htabledesc);
			logger.info("successfully created table '" + tableName + "'");
		}
	}

	/**
	 * Delete a table
	 */
	public static void deleteTable(String tableName) throws Exception {
		HTableDescriptor htabledesc = new HTableDescriptor(TableName.valueOf(tableName));
		if (admin.tableExists(htabledesc.getTableName())) {
			try {
				admin.disableTable(TableName.valueOf(tableName));
				admin.deleteTable(TableName.valueOf(tableName));
				logger.info("successfully deleted table '" + tableName + "'");
			}
			catch (MasterNotRunningException e) {
				logger.error("error while deleting the table '"+tableName+"' : HBase Master node is not running");
				logger.error(e.getMessage());
				e.printStackTrace();
			} 
			catch (ZooKeeperConnectionException e) {
				logger.error("error while deleting the table '"+tableName+"' : error in the ZooKeeper connection");
				logger.error(e.getMessage());
				e.printStackTrace();
			}
		}
		else{
			logger.warn("the table '" + tableName + "' selected for deletion does not exist");
		}
	}

	/**
	 * Put (or insert) a row
	 */
	public static void addRecord(String tableName, String rowKey, String family, String qualifier, String value) {
		try {
			Table table = conn.getTable(TableName.valueOf(tableName));
			Put put = new Put(Bytes.toBytes(rowKey));
			put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
			table.put(put);
			logger.info("successfully inserted record for key: "+rowKey+" : [" + family + ":("+qualifier+":"+value+")] into table " + tableName);
		}
		catch (IOException e) {
			logger.error("error while inserting record " + rowKey + " into table " + tableName +"' : "+e.getMessage());
			e.printStackTrace();
		}
	}

	/**
	 * Delete a row
	 */
	public static void deleteRecord(String tableName, String rowKey) {
		Table table;
		try {
			table = conn.getTable(TableName.valueOf(tableName));
			List<Delete> list = new ArrayList<Delete>();
			Delete del = new Delete(rowKey.getBytes());
			list.add(del);
			table.delete(list);
			logger.info("successfully deleted recored " + rowKey + " from table " + tableName);
		} catch (IOException e) {
			logger.error("error while deleting recored " + rowKey + " from table " + tableName+" : "+e.getMessage());
			e.printStackTrace();
		}
	}

	/**
	 * Get a row
	 */
	public static void getOneRecord(String tableName, String rowKey) throws IOException {
		Table table = conn.getTable(TableName.valueOf(tableName));
		Get get = new Get(rowKey.getBytes());
		Result rs = table.get(get);
		for(Cell cell : rs.rawCells()){

			// same time in millis
			Instant fromEpochMilli = Instant.ofEpochMilli(cell.getTimestamp());
			fromEpochMilli.toString();
			
			logger.info( Bytes.toString(CellUtil.cloneRow(cell))+ " "  + 
				Bytes.toString(CellUtil.cloneFamily(cell))   + ":" +
				Bytes.toString(CellUtil.cloneQualifier(cell)) + " " +
				Bytes.toString(CellUtil.cloneValue(cell)) + 
				" (ts:"+fromEpochMilli.toString()+ ")" ) ;
		}
	}
	/**
	 * Scan (or list) a table
	 */
	public static void getAllRecord (String tableName) {
		try{
			Table table = conn.getTable(TableName.valueOf(tableName));
			Scan s = new Scan();
			ResultScanner ss = table.getScanner(s);
			for(Result r:ss){
				for(Cell cell : r.rawCells()){
					
					// same time in millis
					Instant fromEpochMilli = Instant.ofEpochMilli(cell.getTimestamp());
					fromEpochMilli.toString();
					
					logger.info( Bytes.toString(CellUtil.cloneRow(cell))+ " "  + 
						Bytes.toString(CellUtil.cloneFamily(cell))   + ":" +
						Bytes.toString(CellUtil.cloneQualifier(cell)) + " " +
						Bytes.toString(CellUtil.cloneValue(cell)) + 
						" (ts:"+fromEpochMilli.toString()+ ")" ) ;
				}
			}
		}
		catch (IOException e){
			e.printStackTrace();
		}
	}

	public static void main(String[] agrs) {
		try {

			String tablename = "scores";
			String[] families = { "grade", "course" };

			HBaseTest.createTable(tablename, families);

			// add record Andrea Muti
			HBaseTest.addRecord(tablename, "Andrea Muti", "grade", "", "5");
			HBaseTest.addRecord(tablename, "Andrea Muti", "course", "", "90");
			HBaseTest.addRecord(tablename, "Andrea Muti", "course", "math", "97");
			HBaseTest.addRecord(tablename, "Andrea Muti", "course", "art", "87");

			// add record Alice Di Stefano
			HBaseTest.addRecord(tablename, "Alice Di Stefano", "grade", "", "4");
			HBaseTest.addRecord(tablename, "Alice Di Stefano", "course", "math", "89");

			logger.info("===========get one record========");
			HBaseTest.getOneRecord(tablename, "Andrea Muti");

			logger.info("===========show all record========");
			HBaseTest.getAllRecord(tablename);

			logger.info("===========delete one record========");
			HBaseTest.deleteRecord(tablename, "Alice Di Stefano");

			logger.info("===========show all record========");
			HBaseTest.getAllRecord(tablename);

		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}
}