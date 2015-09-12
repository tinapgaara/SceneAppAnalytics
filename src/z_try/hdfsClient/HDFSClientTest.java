package z_try.hdfsClient;

import maxA.common.Constants;

import java.io.IOException;


public class HDFSClientTest {

	private static String TEST_HDFS_CLIENT_READ_FILE_PATH = "/15-06-22/Give-events.1435004931910";
	private static String TEST_DATE = "/15-06-22/";
	private static String TEST_WRITE_FILE_NAME = "testWriteFile";

	public static void main(String[] args) throws IOException{ (new HDFSClientTest()).testWriteFile();}

	/**
	 * Read binary file from HDFS
	 * @return
	 */
	public void testReadFile(){
		System.out.println("---------------- [testReadFile] BEGIN ...");
		String source = Constants.HDFS_PATH + TEST_HDFS_CLIENT_READ_FILE_PATH;
		HDFSClient client = HDFSClient.getInstance();
		try{
			byte[] bytes = client.readFile(source);

			for(byte b : bytes){
				System.out.println(b);
			}
		} catch(IOException e){
			e.printStackTrace();




		}
		finally {
			System.out.println("---------------- [testReadFile] END .");
		}
	}

	/**
	 * Write binary array to HDFS as a binary file
	 * @return
	 * @throws IOException
	 */
	public void testWriteFile() throws IOException{
		System.out.println("---------------- [testWriteFile] BEGIN ...");
		String hdfsDir = Constants.HDFS_PATH + TEST_DATE ;
		byte[] content = new byte[101];
		for (int i = 0; i < content.length; i++) {
			content[i] = (byte) (i + 10);
		}
		HDFSClient.getInstance().writeBinaryArr(content, TEST_WRITE_FILE_NAME, hdfsDir);

		System.out.println("---------------- [testWriteFile] END .");
	}

}
