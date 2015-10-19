package main.java.uk.ac.imperial.lsds.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

public class Utils {
	
	
	public static byte[] sizeOf(Object obj) {
		ByteArrayOutputStream byteObject = new ByteArrayOutputStream();
		ObjectOutputStream objectOutputStream;
		try {
			objectOutputStream = new ObjectOutputStream(byteObject);
			objectOutputStream.writeObject(obj);
			objectOutputStream.flush();
			objectOutputStream.close();
			byteObject.close();
		} catch (IOException e) {
			System.err.println("Error converting Object to Byte Stream");
			e.printStackTrace();
		}
		return byteObject.toByteArray();
	}

}
