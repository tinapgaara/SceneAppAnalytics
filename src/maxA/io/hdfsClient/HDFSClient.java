package maxA.io.hdfsClient;

import maxA.common.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;

import java.io.*;

public class HDFSClient {

	private static HDFSClient m_instance = null;

	private HDFSClient() {
		// nothing to do here
	}

	public static HDFSClient getInstance() {

		if (m_instance == null) {
			m_instance = new HDFSClient();
		}
		return m_instance;
	}

	public static Configuration getConfig() {

		Configuration config = new Configuration();
		config.set(Constants.FS_DEFAULT_NAME, Constants.FS_DEFAULT_VALUE);
		config.set(Constants.DFS_NAMENODE_NAME, Constants.DFS_NAMENODE_VALUE);
		config.set(Constants.DFS_PERMISSION_GROUP, Constants.DFS_PERMISSION_GROUP_VALUE);
		config.set(Constants.DFS_WEBHDFS_ENABLED, Constants.DFS_WEBHDFS_ENABLED_VALUE);
		return config;
	}

	public byte[] readFile(String file) throws IOException {

		Configuration conf = this.getConfig();

		FileSystem fileSystem = FileSystem.get(conf);

		Path path = new Path(file);
		if (!fileSystem.exists(path)) {
			System.out.println("File " + file + " does not exists");
			return null;
		}
		else {
			System.out.println("File " + file + " exists");
		}

		FSDataInputStream in = fileSystem.open(path);

		String filename = file.substring(file.lastIndexOf('/') + 1,
				file.length());

		byte[] bytes = new byte[1024];

		int numBytes = 0;
		int lenByte = 0;
		while ((numBytes = in.read(bytes)) > 0) {
			lenByte = numBytes;
		}

		byte[] newBytes = new byte[lenByte];
		for(int i = 0 ; i < lenByte ; i++){
			newBytes[i] = bytes[i];
		}

		in.close();
		fileSystem.close();

		return newBytes;
	}

	public boolean ifExists(Path source) throws IOException {

		Configuration config = this.getConfig();

		FileSystem hdfs = FileSystem.get(config);
		boolean isExists = hdfs.exists(source);
		return isExists;
	}

	public void getModificationTime(String source) throws IOException {

		Configuration conf = this.getConfig();

		FileSystem fileSystem = FileSystem.get(conf);
		Path srcPath = new Path(source);

		// Check if the file already exists
		if (!(fileSystem.exists(srcPath))) {
			System.out.println("No such destination " + srcPath);
			return;
		}
		// Get the filename out of the file path
		String filename = source.substring(source.lastIndexOf('/') + 1,
				source.length());

		FileStatus fileStatus = fileSystem.getFileStatus(srcPath);
		long modificationTime = fileStatus.getModificationTime();

		System.out.format("File %s; Modification time : %0.2f %n", filename,
				modificationTime);
	}

	public void copyFromLocal(String source, String dest) throws IOException {

		Configuration conf = this.getConfig();

		FileSystem fileSystem = FileSystem.get(conf);
		Path srcPath = new Path(source);
		Path dstPath = new Path(dest);
		// Check if the file already exists
		if (!(fileSystem.exists(dstPath))) {
			System.out.println("No such destination " + dstPath);
			return;
		}
		// Get the filename out of the file path
		String filename = source.substring(source.lastIndexOf('/') + 1,
				source.length());
		try {
			fileSystem.copyFromLocalFile(srcPath, dstPath);
			System.out.println("File " + filename + "copied to " + dest);
		}
		catch (Exception e) {
			System.err.println("Exception caught! :" + e);
			System.exit(1);
		}
		finally {
			fileSystem.close();
		}
	}

	public void copyToLocal(String source, String dest) throws IOException {

		Configuration conf = this.getConfig();

		FileSystem fileSystem = FileSystem.get(conf);
		Path srcPath = new Path(source);

		Path dstPath = new Path(dest);
		// Check if the file already exists
		if (!(fileSystem.exists(srcPath))) {
			System.out.println("No such destination " + srcPath);
			return;
		}
		// Get the filename out of the file path
		String filename = source.substring(source.lastIndexOf('/') + 1,
				source.length());
		try {
			fileSystem.copyToLocalFile(srcPath, dstPath);
			System.out.println("File " + filename + "copied to " + dest);
		}
		catch (Exception e) {
			System.err.println("Exception caught! :" + e);
			System.exit(1);
		}
		finally {
			fileSystem.close();
		}
	}

	public void renameFile(String fromthis, String tothis) throws IOException {

		Configuration conf = this.getConfig();

		FileSystem fileSystem = FileSystem.get(conf);
		Path fromPath = new Path(fromthis);
		Path toPath = new Path(tothis);

		if (!(fileSystem.exists(fromPath))) {
			System.out.println("No such destination " + fromPath);
			return;
		}

		if (fileSystem.exists(toPath)) {
			System.out.println("Already exists! " + toPath);
			return;
		}

		try {
			boolean isRenamed = fileSystem.rename(fromPath, toPath);
			if (isRenamed) {
				System.out.println("Renamed from " + fromthis + "to " + tothis);
			}
		}
		catch (Exception e) {
			System.out.println("Exception :" + e);
			System.exit(1);
		}
		finally {
			fileSystem.close();
		}
	}

	public void addFile(String source, String dest) throws IOException {
		// Conf object will read the HDFS configuration parameters
		Configuration conf = this.getConfig();

		FileSystem fileSystem = FileSystem.get(conf);
		// Get the filename out of the file path
		String filename = source.substring(source.lastIndexOf('/') + 1,
				source.length());
		// Create the destination path including the filename.
		if (dest.charAt(dest.length() - 1) != '/') {
			dest = dest + "/" + filename;
		}
		else {
			dest = dest + filename;
		}
		// Check if the file already exists
		Path path = new Path(dest);
		if (fileSystem.exists(path)) {
			System.out.println("File " + dest + " already exists");
			return;
		}
		// Create a new file and write data to it.
		FSDataOutputStream out = fileSystem.create(path);
		InputStream in = new BufferedInputStream(new FileInputStream(
				new File(source)));
		byte[] bytes = new byte[1024];
		int numBytes = 0;
		while ((numBytes = in.read(bytes)) > 0) {
			out.write(bytes, 0, numBytes);
		}

		// Close all the file descripters
		in.close();
		out.close();
		fileSystem.close();
	}

	public void writeBinaryArr(byte[] bytes, String filename, String dest) throws IOException {
		// Conf object will read the HDFS configuration parameters
		Configuration conf = this.getConfig();

		FileSystem fileSystem = FileSystem.get(conf);
		// Create the destination path including the filename.
		if (dest.charAt(dest.length() - 1) != '/') {
			dest = dest + "/" + filename;
		}
		else {
			dest = dest + filename;
		}

		// Check if the file already exists
		Path path = new Path(dest);
		if (fileSystem.exists(path)) {
			System.out.println("File " + dest + " already exists");
			return;
		}

		// Create a new file and write data to it.
		FSDataOutputStream out = fileSystem.create(path);

		out.write(bytes);
		out.close();
	}

	public void deleteFile(String file) throws IOException {

		Configuration conf = this.getConfig();

		FileSystem fileSystem = FileSystem.get(conf);

		Path path = new Path(file);
		if (!fileSystem.exists(path)) {
			System.out.println("File " + file + " does not exists");
			return;
		}

		fileSystem.delete(new Path(file), true);
		fileSystem.close();
	}

	public void mkdir(String dir) throws IOException {

		Configuration conf = this.getConfig();
		FileSystem fileSystem = FileSystem.get(conf);

		Path path = new Path(dir);
		if (fileSystem.exists(path)) {
			System.out.println("Dir " + dir + " already exists!");
			return;
		}
	}
}