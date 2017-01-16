package muti.hbase.examples;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;

public class TableManager {
	
	private Connection connection;

	public TableManager(Connection connection){
		this.connection = connection;
	}
	
	public void createTable(String name, List<String> columnFamilyNames){
		HTableDescriptor htable = new HTableDescriptor(TableName.valueOf(name));
		for(String cf : columnFamilyNames){
			htable.addFamily( new HColumnDescriptor(cf));
		}
	  
	    Admin hbase_admin = null;
		try {
			hbase_admin = connection.getAdmin();
		} catch (IOException e) {
			System.err.println("error while getting hbase admin");
		}
		try {
			hbase_admin.createTable(htable);
		} catch (IOException e1) {
			System.err.println("error while creating the table");
		}
		
		try {
			hbase_admin.close();
		} catch (IOException e) {
			System.err.println("error while closing hbase admin");
		}
	}
	
	public Table getTable(String tabName){
		try {
			return connection.getTable(TableName.valueOf(tabName));
		} catch (IOException e) {
			return null;
		}
	}
}
