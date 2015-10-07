package test.java.uk.ac.imperial.lsds.cassandra;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

public class testDatastaxDriver {

	private static Cluster cluster;
	private static Session session;
	private static Double operations = 0.0;
	private static Double totalOperations = 0.0;
	
	private static ArrayList<Long> stats = new ArrayList<Long>();

	public testDatastaxDriver() {

	}
	
	
	private static Long getOneUser() {
		long start = System.currentTimeMillis();

		Statement select = QueryBuilder.select().from("play_cassandra", "users").where(QueryBuilder.eq("key", "pangaref@example.com"));
		ResultSet results = session.execute(select);
		
		assert(results.all().size() == 1);
		
		long end = System.currentTimeMillis();
		// System.out.println("Got All users: " + results.all().size()
		// + "# took: " + (end - start));
		operations++;

		return (end - start);

	}

	private static Long getAllUsers() {
		long start = System.currentTimeMillis();

		Select select = QueryBuilder.select().all().from("play_cassandra", "users");
		ResultSet results = session.execute(select);
		
		long end = System.currentTimeMillis();
		// System.out.println("Got All users: " + results.all().size()
		// + "# took: " + (end - start));
		operations++;

		return (end - start);

	}

	private static Long getAllUsersAsync() {
		long start = System.currentTimeMillis();

		Select select = QueryBuilder.select().all().from("play_cassandra", "users");
		ResultSetFuture results = session.executeAsync(select);
		results.getUninterruptibly().all().size();
		
		long end = System.currentTimeMillis();
		// System.out.println("Got All users: " + results.all().size()
		// + "# took: " + (end - start));
		operations++;

		return (end - start);

	}

	private static void waitHere(long millis) {
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
			System.err.println("Thread sleep failed!");
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		PoolingOptions poolingOptions = new PoolingOptions();
		// customize options...
		poolingOptions.setCoreConnectionsPerHost(HostDistance.LOCAL, 4)
				.setMaxConnectionsPerHost(HostDistance.LOCAL, 10)
				.setCoreConnectionsPerHost(HostDistance.REMOTE, 2)
				.setMaxConnectionsPerHost(HostDistance.REMOTE, 4);

		// Connect to the cluster and keyspace "demo"
		cluster = Cluster.builder()
				// .addContactPoint("wombat11.doc.res.ic.ac.uk")
				.addContactPoint("155.198.198.12")
				.withPoolingOptions(poolingOptions).build();
		session = cluster.connect();

		ExecutorService executor = Executors.newFixedThreadPool(8);
		int roundsCount = 100;

		while (--roundsCount >= 0) {
			System.out.println("");
			System.out.println("Test Started operations/sec = "
					+ testDatastaxDriver.operations);
			totalOperations += testDatastaxDriver.operations;
			testDatastaxDriver.operations = 0.0;

			waitHere(1000);

			for (int i = 0; i < 1000; i++) {
				// System.out.println("Executor " + i);
				executor.execute(new Runnable() {

					@Override
					public void run() {
						stats.add(getOneUser());
					}
				});
			}
		}
		executor.shutdownNow();
		while(!executor.isTerminated()){
			System.out.println("Waiting for termination");
			waitHere(1000);
		}
		
		assert(stats == null );
		
		System.out.println(stats);
		
		List<Long> syncStats = Collections.synchronizedList(stats);
		System.out.println(syncStats.size());
		Collections.sort(syncStats);
		
		System.out.println("Size " + stats.size() + " "
				 + (stats.size() * 1 / 2) + " " + (stats.size() * 9 / 10) + " "
				 + (stats.size() * 99 / 100));

		System.out.println("Query Avg time: " + stats.get(stats.size() * 1 / 2)
				+ "\t 90perc " + stats.get(stats.size() * 9 / 10)
				+ "\t 99perc: " + stats.get(stats.size() * 99 / 100)
				+ "\t Avg Throughput " + (totalOperations / 100 ));

		// Clean up the connection by closing it
		cluster.close();

	}

}
