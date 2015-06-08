package test.java.uk.ac.imperial.lsds.play2sdg;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;

public class TestCassandraCluster {
	
	private Cluster cluster;
	
	public TestCassandraCluster() {
		// TODO Auto-generated constructor stub
	}
	
	public void connect(String node) {
		   cluster = Cluster.builder()
		         .addContactPoint(node).build();
		   Metadata metadata = cluster.getMetadata();
		   System.out.printf("Connected to cluster: %s\n", 
		         metadata.getClusterName());
		   for ( Host host : metadata.getAllHosts() ) {
		      System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
		         host.getDatacenter(), host.getAddress(), host.getRack());
		   }
	}
	
	public void close() {
		   cluster.close();
	}

	
	public static void main(String[] args) {
			TestCassandraCluster client = new TestCassandraCluster();
		   client.connect("127.0.0.1");
		   client.close();
		}

}
