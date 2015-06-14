package main.java.uk.ac.imperial.lsds.io_handlers;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import main.java.uk.ac.imperial.lsds.cassandra.CassandraQueryController;
import main.java.uk.ac.imperial.lsds.models.User;

public class UserFileParser {

	// The name of the file to open.
	private String fileName;
	private boolean isHDFS;
	static Logger logger = Logger
			.getLogger("main.java.uk.ac.imperial.lsds.utils.FileParser");

	public UserFileParser(String fname) {
		this.fileName = fname;		
		if(fileName.startsWith("hdfs"))
			this.isHDFS= true;
		else
			this.isHDFS= false;
	}

	public List<String> ParseLines() {
		List<String> list = new ArrayList<String>();

		// This will reference one line at a time
		String line = null;
		try {
			// FileReader reads text files in the default encoding.
			FileReader fileReader = new FileReader(fileName);

			// Always wrap FileReader in BufferedReader.
			BufferedReader bufferedReader = new BufferedReader(fileReader);

			while ((line = bufferedReader.readLine()) != null) {
				list.add(line);
				System.out.println(line);
			}

			// Always close files.
			bufferedReader.close();
		} catch (FileNotFoundException ex) {
			System.out.println("Unable to open file '" + fileName + "'");
		} catch (IOException ex) {
			System.out.println("Error reading file '" + fileName + "'");
			ex.printStackTrace();
		}

		return list;
	}
	
	

	public List<User> ParseFSUsers() {
		List<User> users = new ArrayList<User>();

		// This will reference one line at a time
		String line = null;
		try {
			// FileReader reads text files in the default encoding.
			FileReader fileReader = new FileReader(fileName);

			// Always wrap FileReader in BufferedReader.
			BufferedReader bufferedReader = new BufferedReader(fileReader);

			while ((line = bufferedReader.readLine()) != null) {
				if (line.startsWith("#"))
					continue;
				else {
					String[] split = line.split("\\s");
					if (split.length < 5) {
						logger.warn("Wrong number of arguments at line: "
								+ line + " skipping line...");
					} else
						users.add(new User(split[0], split[1], split[2],
								split[3], split[4]));
				}
				System.out.println(line);
			}

			// Always close files.
			bufferedReader.close();

		} catch (FileNotFoundException ex) {
			System.out.println("Unable to open file '" + fileName + "'");
		} catch (IOException ex) {
			System.out.println("Error reading file '" + fileName + "'");
			ex.printStackTrace();
		}

		return users;
	}
	
	
	public List<User> ParseHDFSUsers() {
		List<User> users = new ArrayList<User>();
		try {
			HDFSFileBrowser hd = new HDFSFileBrowser(this.fileName);
			List<String> filelines = HDFSFileBrowser.getFileLines(new Path(this.fileName));
			for(String line : filelines ){
				if (line.startsWith("#"))
					continue;
				else {
					String[] split = line.split("\\s");
					if (split.length < 5) {
						logger.warn("Wrong number of arguments at line: "
								+ line + " skipping line...");
					} else
						users.add(new User(split[0], split[1], split[2],
								split[3], split[4]));
				}
				System.out.println(line);
			}
			
		} catch (IOException e) {
			System.out.println("Error reading file '" + fileName + "'");
			e.printStackTrace();
		}
		return users;
		
		
		
	}

	
	public List<User> ParseUsers() {
		if(isHDFS)
			return ParseHDFSUsers();
		else
			return ParseFSUsers();
	}
	
	
	
//	public static void main(String[] args) {
//		UserFileParser userFileParser = new UserFileParser("hdfs://wombat30.doc.res.ic.ac.uk:8020/user/pg1712/users.txt");
//		List<User> usersFromFile = userFileParser.ParseUsers(); 
//
////ALTER KEYSPACE play_cassandra WITH REPLICATION =   { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };
////		CassandraQueryController.UpdateKeyspaceRF("play_cassandra", 3);
//		for(User u : usersFromFile){
//			System.out.println("\n Read user: "+ u);
//			CassandraQueryController.persist(u);
//			System.out.println("Persisted ...");
//		}
//	}

}
