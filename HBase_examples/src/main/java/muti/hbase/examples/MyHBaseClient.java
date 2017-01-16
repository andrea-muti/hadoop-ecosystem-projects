package muti.hbase.examples;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


/**
 * HBase example
 *
 */
public class MyHBaseClient  {
	
	Logger logger;

	private Configuration configuration;
	public MyHBaseClient(Configuration config){
		this.configuration = config;
	}


	public boolean checkConnection(){
		Connection connection;
		try {
			System.out.println(" - calling ConnectionFactory.createConnection");
			connection = ConnectionFactory.createConnection(this.configuration);
			Admin admin = connection.getAdmin();
			ClusterStatus status = admin.getClusterStatus();
			double avgLoad = status.getAverageLoad();
			System.out.println(" - avgLoad : "+avgLoad);
			String clusterId = status.getClusterId();
			System.out.println(" - clusterID : "+clusterId);
			String versionHbase = status.getHBaseVersion();
			System.out.println(" - HBase version : "+versionHbase);
			ServerName serverNameObj = status.getMaster();
			String serverName = serverNameObj.getServerName();
			System.out.println(" - server name : "+serverName);
			String serverHostPort = serverNameObj.getHostAndPort();
			System.out.println(" - server host and port : "+serverHostPort);
			int numRegions = status.getRegionsCount();
			System.out.println(" - number of regions : "+numRegions);
			Collection<ServerName> servers = status.getServers();
			System.out.println(" - Servers Collection:");
			for (ServerName s : servers){
				System.out.println("\t - "+s.toShortString());
			}
			int serversSize = status.getServersSize();
			System.out.println(" - server size : "+serversSize);
			admin.close();
			connection.close();
			return true;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}

	}

	public void createSchemaTables(String tableName, List<String> cfNames, boolean overwrite) throws IOException {
		try {
			Connection connection = ConnectionFactory.createConnection(this.configuration);
			Admin admin = connection.getAdmin();
			HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));
			for(String cf : cfNames){
				table.addFamily(new HColumnDescriptor(cf).setCompressionType(Algorithm.NONE));
			}
			logger.info("Creating table '"+tableName+"'");

			boolean res;
			if (overwrite) {
				res = createOrOverwrite(admin, table);
			}
			else{
				res = create(admin, table);
			}
			
			if (res) {
				logger.info("successfully created table '"+tableName+"'");
			}
			else{
				logger.error("error while creating table '"+tableName+"'");
			}
			
			admin.close();
			connection.close();
		}
		catch(Exception e){
			System.err.println("error : "+e.getMessage());
		}
	}

	private boolean createOrOverwrite(Admin admin, HTableDescriptor table) {
		try {
			if (admin.tableExists(table.getTableName())) {
				logger.info("table '"+table.getTableName()+"' already exists: first deleting it");
				admin.disableTable(table.getTableName());
				admin.deleteTable(table.getTableName());
			}
			admin.createTable(table);
			logger.info("table '"+table.getTableName()+"' successfully created");
			return true;
		}
		catch (Exception e) {
			logger.error("error while creating table '"+table.getTableName()+"': "+e.getMessage());
			return false;
		}
	}

	private boolean create(Admin admin, HTableDescriptor table) {
		try {
			if (!admin.tableExists(table.getTableName())) {
				admin.createTable(table);
				logger.info("successfully created table '"+table.getTableName().getNameAsString()+"'");
				return true;
			}
			else{
				logger.error("error while creating table '"+table.getTableName().getNameAsString()+"' : table already exists");
				return false;
			}
		} catch (Exception e) {
			logger.error("error while creating table '"+table.getTableName().getNameAsString()+"' : "+e.getMessage());
			return false;
		}
	}

	public static void modifySchema(Configuration config, String tabName, String cfName) throws IOException {
		try (Connection connection = ConnectionFactory.createConnection(config);
				Admin admin = connection.getAdmin()) {

			TableName tableName = TableName.valueOf(tabName);
			if (!admin.tableExists(tableName)) {
				System.out.println("Table does not exist.");
				System.exit(-1);
			}

			HTableDescriptor table = admin.getTableDescriptor(tableName);

			// Update existing table
			HColumnDescriptor newColumn = new HColumnDescriptor("NEWCF");
			newColumn.setCompactionCompressionType(Algorithm.GZ);
			newColumn.setMaxVersions(HConstants.ALL_VERSIONS);
			admin.addColumn(tableName, newColumn);

			// Update existing column family
			HColumnDescriptor existingColumn = new HColumnDescriptor(cfName);
			existingColumn.setCompactionCompressionType(Algorithm.GZ);
			existingColumn.setMaxVersions(HConstants.ALL_VERSIONS);
			table.modifyFamily(existingColumn);
			admin.modifyTable(tableName, table);

			// Disable an existing table
			admin.disableTable(tableName);

			// Delete an existing column family
			admin.deleteColumn(tableName, cfName.getBytes("UTF-8"));

			// Delete a table (Need to be disabled first)
			admin.deleteTable(tableName);
		}
	}

	private void configureLogging(String level){
		org.apache.log4j.BasicConfigurator.configure();
		Logger.getRootLogger().setLevel(Level.toLevel(level));
		this.logger = Logger.getLogger(MyHBaseClient.class);
	}

	@SuppressWarnings("unused")
	public static void main(String[] args) throws IOException {

		
		System.setProperty("hadoop.home.dir", "C:\\Users\\Andrea\\hadoop-common-2.2.0-bin-master");

		String zookeeperIP = "192.168.177.101";
		String zookeeperPort = "2181";
		String hbaseMasterAddress = "192.168.177.101";
		String hbaseMasterPort = "160010";

		Configuration config = HBaseConfiguration.create();
		config.clear();
		config.set("hbase.master", hbaseMasterAddress+":160010");
		config.set("hbase.zookeeper.quorum", zookeeperIP);

		MyHBaseClient client = new MyHBaseClient(config);
		client.configureLogging("INFO");

		boolean connectionStatus = client.checkConnection();
		if (connectionStatus) {
			System.out.println("connection is OK - creating schema table");

			String tableName = "myTable2";
			List<String> cfNames = new LinkedList<String>();
			cfNames.add("firstColumn");
			cfNames.add("secondColumn");
			cfNames.add("thirdColumn");

			boolean overwrite = true;
			client.createSchemaTables(tableName, cfNames, overwrite);


		}
		else{
			System.err.println("ERROR : connection non available");
		}
	}
}