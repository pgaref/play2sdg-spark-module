package test.java.uk.ac.imperial.lsds.cassandra;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import main.java.uk.ac.imperial.lsds.cassandra.CassandraQueryController;
import main.java.uk.ac.imperial.lsds.dx_controller.CassandraDxQueryController;
import main.java.uk.ac.imperial.lsds.dx_controller.ClusterManager;
import main.java.uk.ac.imperial.lsds.dx_models.User;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;

public class testDatastaxReadQueries {

	private static Double operations = 0.0;
	private static Double totalOperations = 0.0;
	
	private static ArrayList<Long> stats = new ArrayList<Long>();
	private static ClusterManager cm;
	private static CassandraDxQueryController dx;
	private static PreparedStatement statement;

	public testDatastaxReadQueries(String ... nodes) {
		cm = new ClusterManager("play_cassandra", 1, nodes);
		dx = new CassandraDxQueryController(cm.getSession());
		statement = cm.getSession().prepare("SELECT * FROM play_cassandra.users");
	}
	
	
	private Long getOneUser() {
		long start = System.currentTimeMillis();

		Statement select = QueryBuilder.select().from("play_cassandra", "users").where(QueryBuilder.eq("key", "pangaref@example.com"));
		ResultSet results = cm.getSession().execute(select);
		
		assert(results.all().size() == 1);
		
		long end = System.currentTimeMillis();
		// System.out.println("Got All users: " + results.all().size()
		// + "# took: " + (end - start));
		operations++;

		return (end - start);

	}
	

	private Long getAllUsers() {
		
		long start = System.currentTimeMillis();
		//Select select = QueryBuilder.select().all().from("play_cassandra", "users");
		List<User> results = dx.listAllUsers();
		
		long end = System.currentTimeMillis();
		// System.out.println("Got All users: " + results.all().size()
		// + "# took: " + (end - start));
		operations++;

		return (end - start);

	}
	
	private Long getAllUsersPrepared() {
		
		long start = System.currentTimeMillis();
		//Select select = QueryBuilder.select().all().from("play_cassandra", "users");
		
		ResultSet resultSet = cm.getSession().execute(statement.bind());
		
		MappingManager manager = new MappingManager (cm.getSession());
		Mapper<User> userMapper = manager.mapper(User.class);
		List<User> user = userMapper.map(resultSet).all();
		
		long end = System.currentTimeMillis();
		// System.out.println("Got All users: " + results.all().size()
		// + "# took: " + (end - start));
		operations++;

		return (end - start);

	}

	private Long getAllUsersAsync() {
		long start = System.currentTimeMillis();

		//Select select = QueryBuilder.select().all().from("play_cassandra", "users");
		try {
			List<User> results = dx.listAllUsersAsync();
		} catch (InterruptedException | ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
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
		final testDatastaxReadQueries bench = new testDatastaxReadQueries("146.179.131.141");
		
		ExecutorService executor = Executors.newFixedThreadPool(8);
		long experimentStart = System.currentTimeMillis();
		int roundsCount = 100;
		
		while (--roundsCount >= 0) {
			System.out.println("\n Test Started operations/sec = "+ testDatastaxReadQueries.operations);
			totalOperations += testDatastaxReadQueries.operations;
			testDatastaxReadQueries.operations = 0.0;

			waitHere(1000);
			
			for (int i = 0; i < 1000; i++) {
				// System.out.println("Executor " + i);
				executor.execute(new Runnable() {

					@Override
					public void run() {
						/*
						 * Avoid Concurent write issues!
						 */
						synchronized(stats){
							stats.add(bench.getAllUsers());
						}
					}
				});
			}
		}
		long experimentEnd = System.currentTimeMillis();
		
		
		
		
		executor.shutdownNow();
		while(!executor.isTerminated()){
			System.out.println("Waiting for termination");
			waitHere(1000);
		}
		
		Collections.sort(stats, new LongComparator());
//		System.out.println("Size " + stats.size() + " "
//				+ (stats.size() * 1 / 2) + " " + (stats.size() * 9 / 10) + " "
//				+ (stats.size() * 99 / 100));
		System.out.println("Query Avg time: " + stats.get(stats.size() * 1 / 2)
				+ "\t 90perc " + stats.get(stats.size() * 9 / 10)
				+ "\t 99perc: " + stats.get(stats.size() * 99 / 100)
				+ "\t Avg Throughput " + (totalOperations / ((experimentEnd-experimentStart)/1000)));

		// Clean up the connection by closing it
		cm.disconnect();

	}
	
	static class LongComparator implements Comparator<Long>
	 {
	     public int compare(Long l1, Long l2)
	     {
	    	 if(l1 == null ){
	    		 System.err.println("Should never happen!! l1");
	    		 return -1;
	    	 }
	    	 else if(l2 == null){
	    		 System.err.println("Should never happen!! l2");
	    		 return 1;
	    	 }
	    	 else
	    		 return l1.compareTo(l2);
	     }
	 }

}
