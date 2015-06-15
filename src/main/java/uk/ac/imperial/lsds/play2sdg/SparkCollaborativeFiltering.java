package main.java.uk.ac.imperial.lsds.play2sdg;

import java.util.ArrayList;
import java.util.List;

import main.java.uk.ac.imperial.lsds.cassandra.CassandraQueryController;
import main.java.uk.ac.imperial.lsds.io_handlers.LastFMDataParser;
import main.java.uk.ac.imperial.lsds.io_handlers.RatingsFileWriter;
import main.java.uk.ac.imperial.lsds.models.PlayList;
import main.java.uk.ac.imperial.lsds.models.Recommendation;
import main.java.uk.ac.imperial.lsds.models.Stats;
import main.java.uk.ac.imperial.lsds.models.Track;
import main.java.uk.ac.imperial.lsds.models.User;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;

import scala.Tuple2;

public class SparkCollaborativeFiltering {

	private static Logger logger = Logger.getLogger(SparkCollaborativeFiltering.class);
	
	private static String dataset_path = "hdfs://wombat30.doc.res.ic.ac.uk:8020/user/pg1712/lastfm_train";
	
	
	public static void main(String[] args) {
		
		long jobStarted = System.currentTimeMillis();
		SparkConf conf = new SparkConf().set("spark.executor.memory", "3g")
				.setMaster("local")
				//.setMaster("mesos://wombat30.doc.res.ic.ac.uk:5050")
				.setAppName("play2sdg Collaborative Filtering Job");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		/*
		 * First Fetch the Track List 
		*/ 
		//LastFMDataParser parser = new LastFMDataParser( "hdfs://wombat30.doc.res.ic.ac.uk:8020/user/pg1712/lastfm_subset");
		//LastFMDataParser parser = new LastFMDataParser( "data/LastFM/lastfm_subset");
		LastFMDataParser parser = new LastFMDataParser( "data/LastFM/lastfm_train");
		final List<Track> tracksList = parser.parseDataSet(false);
		System.out.println("## Fetched # "+ tracksList.size() +" Tracks ##");
		
		
		/*
		 * Fetch PlayLists From Cassandra  - aka Ratings
		 */
		
		List<PlayList> allplaylists = CassandraQueryController.listAllPlaylists();
		final List<User> allusers = CassandraQueryController.listAllUsers();
		
		System.out.println("## Total Users Fetched # "+ allusers.size() +" ##");
		
		for(User u : allusers)
			System.out.println("U: "+ u);
		
		System.out.println("## Total PlayLists Fetched # "+ allplaylists.size() +" ##");
		
		for(PlayList p : allplaylists)
			System.out.println("P: "+ p);
		
		List<String> ratingList = new ArrayList<String>();
 		/*
		 * Convert IDS and save to HDFS File
		 */
		for(PlayList playList : allplaylists){
			for(String track : playList.getTracks()){
				StringBuilder sb = new StringBuilder();
				sb.append(userIndex(allusers, playList.getUsermail()) + ",");
				sb.append(trackIndex(tracksList, track) + ",");
				sb.append("5.0");
				ratingList.add(sb.toString());
			}
		}
		
		/*
		 * Persist To FS
		 */
		RatingsFileWriter rw = new RatingsFileWriter("./");
		//RatingsFileWriter rw = new RatingsFileWriter("hdfs://wombat30.doc.res.ic.ac.uk:8020/user/pg1712/lastfm_subset");
		rw.persistRatingsFile(ratingList);
		
		// Load and parse the data
		String path = "./ratings.data";
		//String path = "hdfs://wombat30.doc.res.ic.ac.uk:8020/user/pg1712/lastfm_subset/ratings.data";
				
		JavaRDD<String> data = sc.textFile(path);
		JavaRDD<Rating> ratings = data.map(new Function<String, Rating>() {
			public Rating call(String s) {
				String[] sarray = s.split(",");
				return new Rating(Integer.parseInt(sarray[0]), Integer
						.parseInt(sarray[1]), Double.parseDouble(sarray[2]));
			}
		});

		// Build the recommendation model using ALS
		int rank = 10;
		int numIterations = 20;
		MatrixFactorizationModel model = ALS.train(JavaRDD.toRDD(ratings),
				rank, numIterations, 0.01);

		// Evaluate the model on rating data
		JavaRDD<Tuple2<Object, Object>> userProducts = ratings
				.map(new Function<Rating, Tuple2<Object, Object>>() {
					public Tuple2<Object, Object> call(Rating r) {
						return new Tuple2<Object, Object>(r.user(), r.product());
					}
				});

		JavaPairRDD<Tuple2<Integer, Integer>, Double> predictions = JavaPairRDD
				.fromJavaRDD(model
						.predict(JavaRDD.toRDD(userProducts))
						.toJavaRDD()
						.map(new Function<Rating, Tuple2<Tuple2<Integer, Integer>, Double>>() {
							public Tuple2<Tuple2<Integer, Integer>, Double> call(
									Rating r) {
								return new Tuple2<Tuple2<Integer, Integer>, Double>(
										new Tuple2<Integer, Integer>(r.user(),
												r.product()), r.rating());
							}
						}));
		JavaRDD<Tuple2<Double, Double>> ratesAndPreds = JavaPairRDD
				.fromJavaRDD(
						ratings.map(new Function<Rating, Tuple2<Tuple2<Integer, Integer>, Double>>() {
							public Tuple2<Tuple2<Integer, Integer>, Double> call(
									Rating r) {
								return new Tuple2<Tuple2<Integer, Integer>, Double>(
										new Tuple2<Integer, Integer>(r.user(),
												r.product()), r.rating());
							}
						})).join(predictions).values();
		
		double MSE = JavaDoubleRDD.fromRDD(
				ratesAndPreds.map(
						new Function<Tuple2<Double, Double>, Object>() {
							public Object call(Tuple2<Double, Double> pair) {
								Double err = pair._1() - pair._2();
								return err * err;
							}
						}).rdd()).mean();
		
		System.out.println("\n ## Rates and Predictions: "+ predictions.toArray().toString());
		System.out.println("\n ## Mean Squared Error = " + String.format("%2f", MSE));
		
		/*
		 * Finally read results and Write to Cassandra Recommendations Table
		 */
		predictions.foreach(new VoidFunction<Tuple2<Tuple2<Integer, Integer>,Double>>(){
			@Override
			public void call(Tuple2<Tuple2<Integer, Integer>, Double> v1)
					throws Exception {
				//System.out.println("Tupple: "+ v1.toString());
				logger.debug("Creating Recommendation-> user: "+v1._1()._1 + "\t track: " + v1._1()._2 + "\t score: "+v1._2() );
				Recommendation newRec = new Recommendation(allusers.get(v1._1()._1).getEmail());
				newRec.getRecList().put(tracksList.get(v1._1()._2).getTitle(), v1._2());
				CassandraQueryController.persist(newRec);
			}
		});
		
		/*
		 * Update Stats Table
		 */
		Stats sparkJobStats = new Stats("sparkCF");
		sparkJobStats.getStatsMap().put("Job time(ms)", (double)(System.currentTimeMillis()-jobStarted) ); 
		sparkJobStats.getStatsMap().put("Total Predictions", (double) predictions.count());
		sparkJobStats.getStatsMap().put("Mean Squared Error", Double.valueOf(String.format("%2f", MSE)));
		CassandraQueryController.persist(sparkJobStats);
		
		/*
		JavaRDD<Recommendation> recc = predictions.map(new Function<Tuple2<Tuple2<Integer, Integer>, Double>, Recommendation>() {

			@Override
			public Recommendation call(Tuple2<Tuple2<Integer, Integer>, Double> v1)
					throws Exception {
				System.out.println("Tupple: "+ v1.toString());
				System.out.println("arg1: "+v1._1()._1 + "arg2" + v1._1()._2 + "arg3: "+v1._2() );
				return null;
			}

			
		});*/

		// model.save("myModelPath");
		// MatrixFactorizationModel sameModel =
		// MatrixFactorizationModel.load("myModelPath");
	}
	
	/**
	 * 
	 * @param list
	 * @param mail
	 * @return
	 */
	public static int userIndex(List<User> list, String mail){
		for(int i = 0; i < list.size() ; i++ ){
			if(list.get(i).getEmail().equalsIgnoreCase( mail ))
				return i;
		}
		/*
		 * Error Case - Should never Happen!!
		 */
		logger.error(" Retrieving index for not existing User: "+ mail);
		return -1;
		
	}
	
	/**
	 * 
	 * @param list
	 * @param trackTitle
	 * @return
	 */
	public static int trackIndex(List<Track> list, String trackTitle){
		for(int i = 0; i < list.size(); i++ ){
			if(list.get(i).getTitle().equalsIgnoreCase( trackTitle ))
				return i;
		}
		/*
		 * Error Case - Should never Happen!!
		 */
		logger.error(" Retrieving index for not existing Track: "+ trackTitle);
		return -1;
	}
}