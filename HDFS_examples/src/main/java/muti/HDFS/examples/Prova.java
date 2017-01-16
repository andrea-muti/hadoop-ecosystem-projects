package muti.HDFS.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Example application to show how the HDFS file system Java API works
 *
 * @Author Andrea Muti
 */
public class Prova {

	public static final String filename ="dummy.txt";
	public static final String message = "This is the dummy text for test the write to file operation of HDFS";

	public static void main( String[] args ) {

		BasicConfigurator.configure(); 
		Logger.getRootLogger().setLevel(Level.INFO);

		System.setProperty("hadoop.home.dir", "C:\\Users\\Andrea\\hadoop-common-2.2.0-bin-master");

		String hdfsAddress = "hdfs://192.168.177.101:9000";
		String hdfsIp = "192.168.177.101";

		Configuration config = new Configuration();
		config.set("fs.defaultFS", hdfsAddress);
		config.set("dfs.namenode.address", hdfsIp+":50010");
		config.set("dfs.replication", "1");
		config.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		config.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

		//Get the file system instance
		FileSystem fs = null;
		try {
			fs = FileSystem.get(config);
			System.out.println(" - get FileSystem instance");
		} catch (IOException e) {
			System.out.println(" - error while getting FileSystem instance");
			e.printStackTrace();
			System.exit(-1);
		}

		Path filenamePath = new Path(filename);

		try {
			if(fs.exists(filenamePath)) {
				System.out.println(" - file already exists: delete it");
				//Delete Example
				fs.delete(filenamePath, true);
			}

			//Write example
			FSDataOutputStream out = fs.create(filenamePath);
			System.out.println(" - new file created in the HDFS");
			out.writeUTF(message);
			System.out.println(" - content of the new file updated");
			out.close();

			//Read example
			FSDataInputStream in = fs.open(filenamePath);
			System.out.println(" - opening file in order to read its content");
			String messageIn = in.readUTF();
			System.out.println(" - reading the content of the file: "+messageIn);
			in.close();

			// rename the file
			Path renameFilenamePath = new Path("renamed_" + filename);
			fs.rename(filenamePath, renameFilenamePath);
			System.out.println(" - file renamed");


			fs.close();
			System.out.println(" - FileSystem instance closed");

		} catch(IOException ex) {
			System.out.println("Error: " + ex.getMessage());
		}

	}
}