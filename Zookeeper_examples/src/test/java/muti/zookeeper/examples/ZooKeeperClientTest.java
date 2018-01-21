package muti.zookeeper.examples;


import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.junit.AfterClass;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Unit test for ZooKeeperClient
 * 
 * @author Andrea Muti
 * created: 11 dic 2016
 */

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ZooKeeperClientTest {

	public static ZooKeeperClient client;
	public final static String HOST = "192.168.177.101:2181";
	public final static String ZNODE_PATH = "/MyFirstZnode";

	public final static String ZNODE_CHILD1_PATH = ZNODE_PATH+"/FirstChild";
	public final static String ZNODE_CHILD2_PATH = ZNODE_PATH+"/SecondChild";
	
	public final static String ZNODE_CHILD_of_CHILD2_PATH = ZNODE_CHILD2_PATH+"/ChildOfSecondChild";
	
	{	
		client = new ZooKeeperClient();
	}
	
	@Ignore
	@AfterClass
	public static void deleteNodeAndcloseConnection(){
		try {
			client.deleteNode(ZNODE_PATH);
			client.close();
		} catch (InterruptedException | KeeperException e) {
			System.err.println("error while closing zookeeper connection "+e.getMessage());
		}
	}

	/**
	 * test ZooKeeper connection
	 */
	@Ignore
	@Test
	public void test01_testConnection() {
		try {
			client.connect(HOST);
			System.out.println("test connection ok");
			assertTrue( true );
		} catch (Exception e) {
			System.err.println("test failed (exception thrown): "+e.getMessage());
			fail();
		}   
	}

	/**
	 * test creation of Znode
	 */
	@Ignore
	@Test
	public void test02_testCreateZnode() {

		byte[] data =  "My first zookeeper app".getBytes(); // Declare data

		try {
			client.connect(HOST);
			boolean result = client.createZnode(ZNODE_PATH, data);
			assertTrue(result);
		} catch (Exception e) {
			System.err.println("test failed (exception thrown): "+e.getMessage());
			fail();
		}   
	}

	/**
	 * test existence of Znode on an existing znode
	 */
	@Ignore
	@Test
	public void test03_testZnodeExistsTrue() {
		try {
			client.connect(HOST);
			Stat result = client.znodeExists(ZNODE_PATH);
			assertNotNull(result);
		} catch (Exception e) {
			System.err.println("test failed (exception thrown): "+e.getMessage());
			fail();
		}   
	}

	/**
	 * test existence of Znode on a non existing znode
	 */
	@Ignore
	@Test
	public void test04_testZnodeExistsFalse() {
		String nonExistingZnodePath = "/fakeZnode";
		try {
			client.connect(HOST);
			Stat result = client.znodeExists(nonExistingZnodePath);
			assertNull(result);
		} catch (Exception e) {
			System.err.println("test failed (exception thrown): "+e.getMessage());
			fail();
		}   
	}
	/**
	 * test getData data from existing znode
	 */
	@Ignore
	@Test
	public void test05_testGetData() {
		try {
			client.connect(HOST);
			byte[] result = client.getDataFromNode(ZNODE_PATH);
			String data = new String(result, "UTF-8");
			System.out.println("testGetData result: "+data);
			assertNotNull(result);
		} catch (Exception e) {
			System.err.println("test failed (exception thrown): "+e.getMessage());
			fail();
		}   
	}

	/**
	 * test testUpdate data in an existing znode
	 */
	@Ignore
	@Test
	public void test06_testUpdate() {
		byte[] data = "this is the updated data".getBytes();
		try {
			client.connect(HOST);
			client.update(ZNODE_PATH,data);

			byte[] returned = client.getDataFromNode(ZNODE_PATH);

			assertTrue(new String(data, "UTF-8").equals(new String(returned, "UTF-8")));
		} catch (Exception e) {
			System.err.println("test failed (exception thrown): "+e.getMessage());
			fail();
		}   
	}

	/**
	 * test getChildren on a znode without children
	 */
	@Ignore
	@Test
	public void test07_getChildren() {
		try {
			client.connect(HOST);
			List<String> children = client.getChildren(ZNODE_PATH);
			assertNotNull(children);
			assertTrue(children.size() == 0); 
		} catch (Exception e) {
			System.err.println("test failed (exception thrown): "+e.getMessage());
			fail();
		}   
	}

	/**
	 * test getChildren on a znode with children
	 */
	@Ignore
	@Test
	public void test08_getChildrenExisting() {

		byte[] dataChild1 =  "Data of the First Child".getBytes(); 
		byte[] dataChild2 =  "Data of the Second Child".getBytes(); 

		try {
			client.connect(HOST);

			// create two Znodes that are children of the previously created znode
			client.createZnode(ZNODE_CHILD1_PATH, dataChild1);
			client.createZnode(ZNODE_CHILD2_PATH, dataChild2);

			List<String> children = client.getChildren(ZNODE_PATH);
			assertNotNull(children);
			assertTrue(children.size() == 2); 

			if (children.size() == 2) {
				System.out.println("returned children of node "+ZNODE_PATH+" : ");
				for(String s : children){
					System.out.println(" - "+s);
				}
			}

		} catch (Exception e) {
			System.err.println("test failed (exception thrown): "+e.getMessage());
			fail();
		}   
	}
	
	/**
	 * test deleteNode
	 */
	@Ignore
	@Test
	public void test09_testDelete() {
		try {
			client.connect(HOST);
			
			// delete the first child of the main znode
			client.deleteNode(ZNODE_CHILD1_PATH);
			
			// thus it should have 1 remaining child
			int numChildrenOfMainZnode = client.getChildren(ZNODE_PATH).size();
			assertTrue(numChildrenOfMainZnode == 1);
			
			// the removed znode should no longer exist
			Stat result = client.znodeExists(ZNODE_CHILD1_PATH);
			assertNull(result);
		} catch (Exception e) {
			System.err.println("test failed (exception thrown): "+e.getMessage());
			fail();
		}   
	}
	
	/**
	 * test DeleteNodeWithChildren
	 */
	@Ignore
	@Test
	public void test10_testDeleteNodeWithChildren() {
		byte[] data =  "Data of the Node".getBytes(); 
		
		try {
			client.connect(HOST);
			
			// add a child to the second child of the main node
			client.createZnode(ZNODE_CHILD_of_CHILD2_PATH, data);
	
			// delete the second child of the main node
			client.deleteNodeWithChildren(ZNODE_CHILD2_PATH);
			
			// thus the main node should have no remaining child
			int numChildrenOfMainZnode = client.getChildren(ZNODE_PATH).size();
			assertTrue(numChildrenOfMainZnode == 0);
			
			// the removed znode should no longer exist
			Stat result = client.znodeExists(ZNODE_CHILD2_PATH);
			Stat result2 = client.znodeExists(ZNODE_CHILD_of_CHILD2_PATH);
			assertNull(result);
			assertNull(result2);
		} catch (Exception e) {
			System.err.println("test failed (exception thrown): "+e.getMessage());
			fail();
		}   
	}
	
	@Ignore
	@Test
	public void test11_test(){
		String hbaseZnodePath = "/hbase";
		try {
			client.connect(HOST);
			Stat result = client.znodeExists(hbaseZnodePath);
			System.out.println("result: "+result);
			assertNotNull(result);
		} catch (Exception e) {
			System.err.println("test failed (exception thrown): "+e.getMessage());
			fail();
		}   
	}
	
}
