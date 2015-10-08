package test.java.uk.ac.imperial.lsds.cassandra;

import java.util.List;

import main.java.uk.ac.imperial.lsds.io_handlers.UserFileParser;
import main.java.uk.ac.imperial.lsds.models.User;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.google.common.base.Objects;


//CREATE TABLE play_cassandra.datastax_user (
//		   email text PRIMARY KEY,
//		   username text,
//		   password text,
//	       firstname text,
//		   lastname text
//		);

@Table(keyspace = "play_cassandra", name = "users")
public class DatastaxUser {

	@PartitionKey
	@Column(name = "key")
	public String email;

	@Column(name = "username")
	public String username;

	@Column(name = "password")
	public String password;

	@Column(name = "firstname")
	public String firstname;

	@Column(name = "lastname")
	public String lastname;
	
	public DatastaxUser(){}

	public DatastaxUser(String email, String username, String password, String firstname, String lastname){
		this.email = email;
		this.username = username;
		this.password = password;
		this.firstname = firstname;
		this.lastname = lastname;
	}
	
	/**
	 * @return the email
	 */
	public String getEmail() {
		return email;
	}

	/**
	 * @param email
	 *            the email to set
	 */
	public void setEmail(String email) {
		this.email = email;
	}

	/**
	 * @return the username
	 */
	public String getUsername() {
		return username;
	}

	/**
	 * @param username
	 *            the username to set
	 */
	public void setUsername(String username) {
		this.username = username;
	}

	/**
	 * @return the password
	 */
	public String getPassword() {
		return password;
	}

	/**
	 * @param password
	 *            the password to set
	 */
	public void setPassword(String password) {
		this.password = password;
	}

	/**
	 * @return the firstname
	 */
	public String getFirstname() {
		return firstname;
	}

	/**
	 * @param firstname
	 *            the firstname to set
	 */
	public void setFirstname(String firstname) {
		this.firstname = firstname;
	}

	/**
	 * @return the lastname
	 */
	public String getLastname() {
		return lastname;
	}

	/**
	 * @param lastname
	 *            the lastname to set
	 */
	public void setLastname(String lastname) {
		this.lastname = lastname;
	}

	@Override
	public boolean equals(Object other) {
		if (other instanceof DatastaxUser) {
			DatastaxUser that = (DatastaxUser) other;
			return Objects.equal(this.email, that.email)
					&& Objects.equal(this.username, that.username);
		}
		return false;
	}

	@Override
	public int hashCode() {
		System.out.println("My partition key"+ Objects.hashCode(email));
		return Objects.hashCode(email);
	}
	
	public static void main(String[] args) {
		UserFileParser userParser = new UserFileParser("/home/pg1712/workspace/play2sdg-spark-module/data/users.txt");
		List<User> users = userParser.ParseUsers();
		
		
		PoolingOptions poolingOptions = new PoolingOptions();
		// customize options...
		poolingOptions.setCoreConnectionsPerHost(HostDistance.LOCAL, 4)
				.setMaxConnectionsPerHost(HostDistance.LOCAL, 10)
				.setCoreConnectionsPerHost(HostDistance.REMOTE, 2)
				.setMaxConnectionsPerHost(HostDistance.REMOTE, 4);
		// Connect to the cluster and keyspace "demo"
		Cluster	cluster = Cluster.builder()
						//.addContactPoint("wombat11.doc.res.ic.ac.uk")
						.addContactPoint("155.198.198.12")
						.withPoolingOptions(poolingOptions).build();
		Session	session = cluster.connect();
		
		MappingManager manager = new MappingManager(session);
		Mapper<DatastaxUser> mapper = manager.mapper(DatastaxUser.class);
		
		long currTime = System.currentTimeMillis();
		
		for(User u: users){
			DatastaxUser tmp = new DatastaxUser(u.getEmail(), u.getUsername(), u.getPassword(), u.getFistname(), u.getLastname());
			mapper.save(tmp);
			
			if(System.currentTimeMillis() - currTime > 1000){
				System.out.println("Still Pushing data..");
				currTime = System.currentTimeMillis();
			}
		}
		System.out.println("Done?");
		cluster.close();
		
	}
	

}
