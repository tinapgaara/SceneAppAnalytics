package z_try;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * Created by max2 on 6/17/15.
 */
public class Decoder {

    private static Decoder m_instance = null;

    private Decoder(){
        // nothing to do here
    }

    public static Decoder getInstance(){
        if(m_instance == null){
            m_instance =  new Decoder();
        }
        return m_instance;
    }

    public byte[] m_bytes = null;
    public int m_count = 0;

    public void setBytes (byte[] bytes){
        m_bytes = bytes;
    }

    public void setCount (int count) { m_count = count; }
    public int getCount(){
        return m_count;
    }

    public static byte[] readBinaryArrFromFile(String filePath){

        byte[] bytes = null;

        try {
            File file = new File(filePath);

            bytes = new byte[(int) file.length()];
            FileInputStream fileInputStream = new FileInputStream(file);
            fileInputStream.read(bytes);
            fileInputStream.close();

            int count = 0;
            for(byte b:bytes){
                System.out.println(count + "," + b);
                count++;
            }
        }catch(IOException e){
            e.printStackTrace();
        }

        return bytes;
    }
}
