package main.java.uk.ac.imperial.lsds.dx_models;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;

public class PlayClusterManager {

	private Cluster cluster;
	private Session session;

	public Session getSession() {
		return this.session;
	}

	public void connect(String node) {
		PoolingOptions poolingOptions = new PoolingOptions();
		// customize options...
		poolingOptions.setCoreConnectionsPerHost(HostDistance.LOCAL, 4)
				.setMaxConnectionsPerHost(HostDistance.LOCAL, 10)
				.setCoreConnectionsPerHost(HostDistance.REMOTE, 2)
				.setMaxConnectionsPerHost(HostDistance.REMOTE, 4);
		
		cluster = Cluster.builder()
				.addContactPoint(node)
				.withPoolingOptions(poolingOptions)
				.build();
		
		Metadata metadata = cluster.getMetadata();
		System.out.printf("Connected to cluster: %s\n",
				metadata.getClusterName());
		
		for (Host host : metadata.getAllHosts()) {
			System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n",
					host.getDatacenter(), host.getAddress(), host.getRack());
		}
		session = cluster.connect();
	}
	public void disconnect(){
		this.session.close();
		this.cluster.close();
	}
	
	public void createCounterTable(){
		session.execute( 
				"CREATE TABLE  IF NOT EXISTS play_cassandra.counters (" +
			    "key text PRIMARY KEY,"+
			    "counter int" +
			");");
	}
	public void createPlayListsTable(){
		session.execute("CREATE TABLE  IF NOT EXISTS play_cassandra.playlists ("+
			    "id uuid PRIMARY KEY,"+
			    "folder text,"+
			    "tracks list<text>,"+
			    "usermail text"+
			");");
		// Secondary Index
		session.execute("CREATE INDEX  IF NOT EXISTS ON play_cassandra.playlists (usermail);");
	}
	
	public void createUserTable(){
		session.execute("CREATE TABLE  IF NOT EXISTS play_cassandra.users ("+
					"key text PRIMARY KEY,"+
					"firstname text,"+
					"lastname text,"+
					"password text,"+
					"username text"+
				");");
	}
	
	public void createTracksTable(){
		session.execute("CREATE TABLE  IF NOT EXISTS play_cassandra.tracks ("+
				 	"key text PRIMARY KEY,"+
    				"artist text,"+
    				"releaseDate timestamp,"+
    				"title text"+
			");");
		// Secondary Index
		session.execute("CREATE INDEX  IF NOT EXISTS ON play_cassandra.tracks (title);");	
	}
	
	public void createStatSeriesTable(){
		session.execute("CREATE TABLE  IF NOT EXISTS play_cassandra.statseries ("+
					"id text,"+
					"timestamp timestamp,"+
					"doubleVal map<text, text>,"+
					"PRIMARY KEY (id, timestamp)"+
				");");
	}
	
	
	public void createSchema(int replication_factor) {
		session.execute("CREATE KEYSPACE IF NOT EXISTS play_cassandra WITH replication " + 
	            "= {'class':'SimpleStrategy', 'replication_factor':"+replication_factor+"};");
		
		createCounterTable();
		createPlayListsTable();
		createUserTable();
		createTracksTable();
		
		
	}
	
	public void dropSchema() throws Exception{
		throw new Exception("Not implemented yet!");
	}
	
	
	 public void loadData() {
		 
	 }
	 
	 public static void main(String[] args) {
		PlayClusterManager pg = new PlayClusterManager();
		pg.connect("155.198.198.12");
		pg.createSchema(1);
		
		Mapper<User> mapper = new MappingManager(pg.getSession()).mapper(User.class);
		User me = mapper.get("pangaref@example.com");
		System.out.println("got user "+ me);
		//map(pg.getSession().execute("SELECT * FROM play_cassandra.users"));
		
		pg.session.close();
		pg.cluster.close();
		
		
		 
		 
	}
	

}
