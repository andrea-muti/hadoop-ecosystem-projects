package muti.hbase.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

public class LinuxClient {
	
	static final Logger logger = Logger.getLogger(LinuxClient.class);
	
	static final String HBASE_IP = "192.168.177.101";
	static final String ZK_IP = "192.168.177.101";
	
	
	public static void main(String[] args) {
		
		BasicConfigurator.configure();
		
		System.setProperty("hadoop.home.dir", "C:\\Users\\Andrea\\hadoop-common-2.2.0-bin-master");
		
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.master", HBASE_IP+":160010");
		conf.set("hbase.zookeeper.quorum", ZK_IP);
		
		try {           
			Connection conn = ConnectionFactory.createConnection(conf);
			System.out.println("CONNECTED TO HBASE");
		
			System.out.println("CONN TO STRING:"+conn.toString());
			System.out.println("CONF TO STRING:"+conn.getConfiguration().toString());
			
			Admin hAdmin = conn.getAdmin();
			System.out.println("CLUSTER STATUS - hbase versions: "+hAdmin.getClusterStatus().getHBaseVersion());
			System.out.println("               - master:"+hAdmin.getClusterStatus().getMaster().getHostAndPort());
			HTableDescriptor htabledesc = new HTableDescriptor(TableName.valueOf("mycustomer"));
			htabledesc.addFamily(new HColumnDescriptor("name"));
			htabledesc.addFamily(new HColumnDescriptor("contactinfo"));
			htabledesc.addFamily(new HColumnDescriptor("address"));

			hAdmin.createTable(htabledesc);
			
			System.out.println("table created successfully...");
			
			System.out.println("\nenter to delete the table:");
			System.in.read();
			
			hAdmin.disableTable(TableName.valueOf("mycustomer"));
			hAdmin.deleteTable(TableName.valueOf("mycustomer"));
			
			
			System.out.println("table deleted successfully...");

			hAdmin.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

