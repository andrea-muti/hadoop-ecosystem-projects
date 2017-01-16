package muti.HDFS.examples;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * HDFS Client
 * 
 * @author Andrea Muti
 * created: 11 dic 2016
 *
 */

public class HDFSClient {

	private String hdfsIP;
	private String hdfsPort;
	private String hdfsAddress;
	private String replication;

	private Configuration config;

	private FileSystem fs;

	/**
	 * public constructor
	 */
	public HDFSClient(String hdfsIP, String hdfsPort, String replication){
		this.hdfsIP = hdfsIP;
		this.hdfsPort = hdfsPort;
		this.hdfsAddress = "hdfs://"+this.hdfsIP+":"+this.hdfsPort;
		this.replication = replication;

		// Create a default hadoop configuration
		this.config = new Configuration();
		this.config.set("fs.defaultFS", this.hdfsAddress);
		this.config.set("fs.default.name", this.hdfsAddress);
		this.config.set("dfs.namenode.address", this.hdfsIP+":50010");
		this.config.set("dfs.replication", this.replication);
		this.config.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		this.config.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

		// configure logging
		configureLogging("WARN");

		this.fs = getFileSystem();
		if (this.fs == null) {
			System.exit(-1);
		}
	}

	private static void configureLogging(String level){
		org.apache.log4j.BasicConfigurator.configure();
		Logger.getRootLogger().setLevel(Level.toLevel(level));
	}

	private FileSystem getFileSystem(){
		try {
			FileSystem fsys = FileSystem.get(this.config);
			return fsys;
		} catch (IOException e) {
			System.err.println("error while getting the FileSystem: "+e.getMessage());
			return null;
		}
	}

	public boolean fileExists(String path) throws IOException {
		Path filenamePath = new Path(path);

		if (fs.exists(filenamePath)) {
			return true;
		}
		else{
			return false;
		}
	}


	public boolean createFile(String path, boolean overwrite) throws IOException{

		Path filenamePath = new Path(path);

		if (this.fs.exists(filenamePath) && !overwrite){ return false; }
		else{
			try{
				FSDataOutputStream out = this.fs.create(filenamePath,true);
				out.close();
				return true;
			}
			catch (Exception e) {
				return false;
			}
		}
	}

	public boolean writeToFile(String path, String msg) throws IOException {

		//Write Configuration to File
		Path filenamePath = new Path(path);

		if (!fs.exists(filenamePath)){
			return false;
		}

		// Gets output stream for input path using DFS instance
		final FSDataOutputStream streamWriter = fs.create(filenamePath);
		streamWriter.writeUTF(msg);
		streamWriter.flush();
		streamWriter.close();

		return true;
	}

	public String getHomeDirectoryPath(){
		return this.fs.getHomeDirectory().getName();
	}

	public List<String> readFromFile(String filePath) throws IOException {

		//Write Configuration to File
		Path filenamePath = new Path(filePath);

		if (!fs.exists(filenamePath)) {
			return null;
		}

		List<String> result = new LinkedList<String>();

		//FSInputStream to read out of the filenamePath file
		FSDataInputStream fdin = fs.open(filenamePath);
		BufferedReader d = new BufferedReader(new InputStreamReader(fdin));
		String line = d.readLine();
		while (line != null) {
			result.add(line);
			line = d.readLine();
		}
		d.close();
		fdin.close();

		return result;
	}


	

	public boolean close(){
		try {
			this.fs.close();
			return true;
		} catch (IOException e) {
			System.out.println("ERROR ON CLOSE: "+e.getMessage());
			return false;
		}
	}

	public boolean deleteFile(String filePath){
		Path filenamePath = new Path(filePath);
		try{
			boolean res = this.fs.delete(filenamePath, false);
			return res;
		}
		catch (Exception e) {
			return false;
		}
	}

	public static void main( String[] args ) throws IOException {
		System.out.println( "---- HDFS CLIENT ---- ");

		final String REMOTE_USER = "andrea-muti";

		System.setProperty("hadoop.home.dir", "C:\\Users\\Andrea\\hadoop-common-2.2.0-bin-master");
		System.setProperty("HADOOP_USER_NAME", REMOTE_USER);

		final String HDFS_IP = "192.168.177.101";
		final String HDFS_PORT = "9000";
		final String replication = "1";

		final String nonExistingFile = "/andrea-muti/provaHdfs.txt";
		final String newFilePath = "/andrea-muti/theNewFile.txt";
		final String toOverwrite = "/andrea-muti/fileToOverwrite.txt";
		final String inputmsg = "This is the message that was written by the HDFS Java Client!!\n";

		HDFSClient client = new HDFSClient(HDFS_IP, HDFS_PORT, replication);

		String homeDir = client.getHomeDirectoryPath();
		System.out.println(" - home directory : "+homeDir);		

		boolean exists = client.fileExists(nonExistingFile);
		System.out.println(" - the file '"+nonExistingFile+"' exists in the hdfs? "+exists);

		if (exists) {
			List<String> content = client.readFromFile(nonExistingFile);
			for(String s : content) {
				System.out.println(" - content of the file: "+s);
			}
		}

		boolean newExists = client.fileExists(newFilePath);
		System.out.println(" - the file '"+newFilePath+"' exists in the hdfs? "+newExists);


		System.out.println(" - trying to create a new file in the HDFS:");
		try{
			boolean created = client.createFile(newFilePath,true);
			System.out.println("\t - created new file "+newFilePath+" ? "+created);
		}catch (Exception e) {
			System.err.println("\t - ERROR while creating the file:\n"+e.getMessage());
			System.exit(-1);
		}

		List<String> content = client.readFromFile(newFilePath);

		if(content.isEmpty()){
			System.out.println("\t - the file is empty");
		}
		else{
			for(String s : content) {
				System.out.println("\t - content of the file: "+s);
			}
		}


		System.out.println(" - trying to write a message on the file "+newFilePath);
		boolean writeOk = client.writeToFile(newFilePath, inputmsg);
		System.out.println("\t - message written on file? "+writeOk);

		System.out.println(" - trying to read the content of the file "+newFilePath);
		content = client.readFromFile(newFilePath);
		if (content == null){
			System.out.println("\t - the file does not exist");
		}
		else{
			if(content.isEmpty()){
				System.out.println("\t - the file is empty");
			}
			else{
				for(String s : content) {
					System.out.println("\t - content of the file: "+s);
				}
			}
		}

		System.out.print("- deleting the file "+newFilePath+" :");
		boolean delRes = client.deleteFile(newFilePath);
		if(delRes){
			System.out.println("DONE");
		}
		else{
			System.out.println("FAILED");
		}
		
		newExists = client.fileExists(newFilePath);
		System.out.println(" - the file '"+newFilePath+"' exists in the hdfs? "+newExists);
		
		boolean toOverW = client.fileExists(toOverwrite);
		System.out.println(" - the file '"+toOverwrite+"' exists in the hdfs? "+toOverW);
		boolean toOverWCr = client.createFile(toOverwrite, false);
		System.out.println(" - the file '"+toOverwrite+"' created successfully? "+toOverWCr);
		toOverW = client.fileExists(toOverwrite);
		System.out.println(" - now, the file '"+toOverwrite+"' exists in the hdfs? "+toOverW);
		toOverWCr = client.createFile(toOverwrite, true);
		System.out.println(" - the file '"+toOverwrite+"' overwritten successfully? "+toOverWCr);

	
		
		System.out.print(" - closing HDFS client: ");
		if ( client.close() ) {
			System.out.println("DONE");
		}
		else{ System.out.println("FAILED");}
	}

	

}
