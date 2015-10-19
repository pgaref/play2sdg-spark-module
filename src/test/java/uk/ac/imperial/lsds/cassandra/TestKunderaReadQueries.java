package test.java.uk.ac.imperial.lsds.cassandra;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import main.java.uk.ac.imperial.lsds.cassandra.KunderaCassandraQueryController;
import main.java.uk.ac.imperial.lsds.models.Track;
import main.java.uk.ac.imperial.lsds.models.User;

public class TestKunderaReadQueries {

	private static Long testGetAllTracks() {
		long start = System.currentTimeMillis();
		List<Track> all = KunderaCassandraQueryController.listAllTracks();
		long end = System.currentTimeMillis();
		System.out.println("Get all tracks: " + all.size() + "# took: "
				+ (end - start));

		return (end - start);
	}

	private static Long testGetAllUsers() {
		long start = System.currentTimeMillis();
		List<User> all = KunderaCassandraQueryController.listAllUsers();
		long end = System.currentTimeMillis();
		System.out.println("Get all tracks: " + all.size() + "# took: "
				+ (end - start));

		return (end - start);
	}
	
	private static Long testGetOneUser(){
		long start = System.currentTimeMillis();
		User got = KunderaCassandraQueryController.findbyEmail("pgaref@example.com");
		long end = System.currentTimeMillis();
		System.out.println("Get user: " + got + "# took: "
				+ (end - start));

		return (end - start);
	}


	public static void main(String[] args) {

		ArrayList<Long> stats = new ArrayList<Long>();
		int roundsCount = 100;

		while (--roundsCount >= 0) {
			System.out.println("Test Started");
			waitHere(1000);
			stats.add(testGetAllUsers());

		}

		Collections.sort(stats);
//		System.out.println("Size " + stats.size() + " "
//				+ (stats.size() * 1 / 2) + " " + (stats.size() * 9 / 10) + " "
//				+ (stats.size() * 99 / 100));

		System.out.println("Query Avg time: " + stats.get(stats.size() * 1 / 2)
				+ "\t 90perc " + stats.get(stats.size() * 9 / 10) + "\t 99perc: "
				+ stats.get(stats.size() * 99 / 100));

	}
	
	private static void waitHere(long millis) {
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
			System.err.println("Thread sleep failed!");
			e.printStackTrace();
		}
	}


}
