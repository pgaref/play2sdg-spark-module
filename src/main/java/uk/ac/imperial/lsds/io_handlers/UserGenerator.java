package main.java.uk.ac.imperial.lsds.io_handlers;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class UserGenerator {
	
	private Set<String> userSet;
	private String path;
	private int ammount;
	private String userPattern = "user_";
	private static Logger logger = Logger.getLogger(UserGenerator.class);
	
	public UserGenerator(String filename, int ammount) {
		this.path = filename;
		this.ammount = ammount;
		this.userSet = new HashSet<String>();
	}

	private void generateUsers() {
		for (int i = 0; i < this.ammount; i++) {
			String tmpUser = this.userPattern
					+ UUID.randomUUID().toString().replaceAll("-", "");
			userSet.add(tmpUser);
		}
	}

	/**
	 * user2@example.com user2 secret user2Fname user2Lname
	 * 
	 * @param users
	 */
	private void persistFS() {

		try {
			File file = new File(this.path);

			// if file doesnt exists, then create it
			if (!file.exists()) {
				file.createNewFile();
			}

			FileWriter fw = new FileWriter(file.getAbsoluteFile(), true);
			BufferedWriter bw = new BufferedWriter(fw);
			//Add a comment Line to be able to revert changes!
			bw.write("\n## Automatically Generated Data for " + this.ammount +" users ##");
			for (String user : this.userSet) {
				String newline = "\n" + user + "@example.com " + user
						+ " secret " + user + "Fname " + user + "Lname";
				logger.info(" Creating userline:  " + newline.replace("\n", ""));
				bw.write(newline);
			}
			bw.close();
			logger.info(" Persisting user file to '" + path + "' -> Done");

		} catch (IOException e) {
			logger.error("Writing ratings File to normal FS failed! Path : "
					+ path);
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {

		logger.setLevel(Level.DEBUG);
		UserGenerator gen = new UserGenerator("data/users.txt", 10000);
		gen.generateUsers();
		gen.persistFS();
	}

}
