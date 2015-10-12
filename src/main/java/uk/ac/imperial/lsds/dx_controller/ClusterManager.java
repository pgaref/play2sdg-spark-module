package main.java.uk.ac.imperial.lsds.dx_controller;

import java.util.ArrayList;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import main.java.uk.ac.imperial.lsds.dx_models.User;
import main.java.uk.ac.imperial.lsds.play2sdg.PrepareData;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;

public class ClusterManager {

	private Cluster cluster;
	private Session session;
	private int repl_factor;
	private String keyspace;
	private static SchemaManager sm = null;
	private ArrayList<String> cassandraNodes;
	
	private static Logger logger = Logger.getLogger(ClusterManager.class);
	
	public ClusterManager(String kspace, int rf, String ... nodes){
		this.keyspace = kspace;
		this.repl_factor = rf;
		this.cassandraNodes = new ArrayList<String>();
		for(String node: nodes)
			this.cassandraNodes.add(node);
		this.connect(this.cassandraNodes.toArray(new String[cassandraNodes.size()]));
	}
	
	public Session getSession(){
		return this.session;
	}

	public void connect(String ... nodes) {
		PoolingOptions poolingOptions = new PoolingOptions();
		// customize options...
		poolingOptions.setCoreConnectionsPerHost(HostDistance.LOCAL, 4)
				.setMaxConnectionsPerHost(HostDistance.LOCAL, 10)
				.setCoreConnectionsPerHost(HostDistance.REMOTE, 2)
				.setMaxConnectionsPerHost(HostDistance.REMOTE, 4);
		
		cluster = Cluster.builder()
				.addContactPoints(nodes)
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
	
	
	public void createSchema() {
		try {
			if(sm == null)
				sm = new SchemaManager(this.getSession());
			sm.createSchema();
		} catch (Exception e) {
			System.err.println("Cassandra Session error when creating schema.");
			e.printStackTrace();
		}
		logger.info("Schema created");

	}

	public void dropSchema() {
		try {
			if(sm == null)
				sm = new SchemaManager(this.getSession());
			sm.dropSchema();
		} catch (Exception e) {
			System.err.println("Cassandra Session error when creating schema.");
			e.printStackTrace();
		}
	}

	public void loadData() {
		logger.info("Loading Data");
		PrepareData p = new PrepareData("data/users.txt", "data/LastFM/lastfm_train", this.getSession());
		logger.info("Finished Loading data");
	}

	public void disconnect(){
		this.session.close();
		this.cluster.close();
	}
	 
	static
	{
	    Logger rootLogger = Logger.getRootLogger();
	    rootLogger.setLevel(Level.INFO);
	    rootLogger.addAppender(new ConsoleAppender(
	               new PatternLayout("%-6r [%p] %c - %m%n")));
	}
	
	public static void main(String[] args) {
		logger.setLevel(Level.INFO);
		ClusterManager pg = new ClusterManager("play_cassandra", 1, "146.179.131.141");
		//ClusterManager pg = new ClusterManager("play_cassandra", 1, "155.198.198.12");
		pg.createSchema();
		
		//pg.loadData();
		
		Mapper<User> mapper = new MappingManager(pg.getSession()).mapper(User.class);
		User me = mapper.get("pangaref@example.com");
		System.out.println("got user "+ me);
		//map(pg.getSession().execute("SELECT * FROM play_cassandra.users"));
		
		pg.session.close();
		pg.cluster.close();
		
		
		 
		 
	}
	

}