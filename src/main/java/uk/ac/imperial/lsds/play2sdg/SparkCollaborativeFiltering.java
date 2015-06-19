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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

public class SparkCollaborativeFiltering {

	private static Logger logger = LoggerFactory.getLogger(SparkCollaborativeFiltering.class);
	
	private static final String dataset_path = "hdfs://wombat30.doc.res.ic.ac.uk:8020/user/pg1712/lastfm_train";
	//private static final String dataset_path = "data/LastFM/lastfm_subset";
	private static List<PlayList> allplaylists;
	private static List<User> allusers;
	private static List<Track> tracksList;
	
	public static void main(String[] args) {

		long jobStarted = System.currentTimeMillis();
		SparkConf conf = new SparkConf()
				/*	Fraction of memory reserved for caching
				 *	default is 0.6, which means you only get 0.4 * 4g memory for your heap
				 */
				//.set("spark.storage.memoryFraction", "0.1")
				//spark-submit alternative: --driver-memory 2g
				.set("spark.driver.memory", "4g")
				.set("spark.executor.memory","4g")
				.set("spark.driver.maxResultSize","4g")
				.setMaster("local")
				//.setMaster("mesos://wombat30.doc.res.ic.ac.uk:5050")
				.setAppName("play2sdg Collaborative Filtering Job");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		
		
		/*
		 *  Fetch the Track List 
		*/ 
		//LastFMDataParser parser = new LastFMDataParser( "hdfs://wombat30.doc.res.ic.ac.uk:8020/user/pg1712/lastfm_subset");
		//LastFMDataParser parser = new LastFMDataParser( "data/LastFM/lastfm_subset");
		logger.info("## Listing all Tracks Stored at HDFS ## ");
		/*
		 * LastFMDataParser parser = new LastFMDataParser(dataset_path);
		 * final List<Track> tracksList = LastFMDataParser.parseDataSet(false);
		 */
		//tracksList = CassandraQueryController.listAllTracks();
		LastFMDataParser parser = new LastFMDataParser(dataset_path);
		tracksList = LastFMDataParser.parseDataSet(false);
		logger.info("## Fetched # "+ tracksList.size() +" Tracks ##");
		
		/*
		 * Fetch PlayLists From Cassandra  - aka Ratings
		 */
		
		allplaylists = CassandraQueryController.listAllPlaylists();
		allusers = CassandraQueryController.listAllUsers();
		
		logger.info("## Total Users Fetched # "+ allusers.size() +" ##");
		
		for(User u : allusers)
			System.out.println("U: "+ u);
		
		logger.info("## Total PlayLists Fetched # "+ allplaylists.size() +" ##");
		
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
		logger.info("## Converted ratings from: "+allplaylists.size() + " playlists##");
		
		/*
		 * Persist To FS
		 */
		RatingsFileWriter rw = new RatingsFileWriter(dataset_path);
		//RatingsFileWriter rw = new RatingsFileWriter("hdfs://wombat30.doc.res.ic.ac.uk:8020/user/pg1712/lastfm_subset");
		rw.persistRatingsFile(ratingList);
		
		// Load and parse the data
		String path = dataset_path +"/ratings.data";
		//String path = "hdfs://wombat30.doc.res.ic.ac.uk:8020/user/pg1712/lastfm_subset/ratings.data";
		
		logger.info("## Persisting to HDFS -> Done ##");
				
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
		
		System.out.println("\n ## Rates and Predictions Size: "+ predictions.toArray().size());
		System.out.println("\n ## Mean Squared Error = " + String.format("%2f", MSE));
		
		/*
		 * Finally read results and Write to Cassandra Recommendations Table
		 * Avoid Distributeed Spark way! -> Static data issue (allTracks and allUsers Lists)
		 *
		predictions.foreach(new VoidFunction<Tuple2<Tuple2<Integer, Integer>,Double>>(){
			@Override
			public void call(Tuple2<Tuple2<Integer, Integer>, Double> v1)
					throws Exception {
				//System.out.println("Tupple: "+ v1.toString());
				logger.debug("Creating Recommendation-> user: "+v1._1()._1 + "\t track: " + v1._1()._2 + "\t score: "+v1._2() );
				allusers = CassandraQueryController.listAllUsers();
				Recommendation newRec = new Recommendation(allusers.get(v1._1()._1).getEmail());
				tracksList = CassandraQueryController.listAllTracks();
				newRec.getRecList().put(tracksList.get(v1._1()._2).getTitle(), v1._2());
				CassandraQueryController.persist(newRec);
			}
		});*/
		

		MapPredictions2Tracks(predictions);
		
		/*
		 * Update Stats Table
		 */
		Stats sparkJobStats = new Stats("sparkCF");
		sparkJobStats.getStatsMap().put("Job time(s)", Double.parseDouble( ((System.currentTimeMillis()-jobStarted)/1000)+"") ); 
		sparkJobStats.getStatsMap().put("Total Predictions", Double.parseDouble( ""+ predictions.count() ));
		sparkJobStats.getStatsMap().put("Mean Squared Error",  MSE );
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
		//TODO: CHECK THIS
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
	
	/**
	 * Method Mapping generated Recommendations to Tracks and Users 
	 * @param predictions
	 */
	
	public static void MapPredictions2Tracks(JavaPairRDD<Tuple2<Integer, Integer>, Double> predictions){
		for( Tuple2 <Tuple2<Integer, Integer>,Double> pred: predictions.toArray() ){
			logger.debug("Creating Recommendation-> user: "+pred._1()._1 + "\t track: " + pred._1()._2 + "\t score: "+pred._2() );
			Recommendation newRec = new Recommendation(allusers.get(pred._1()._1).getEmail());
			newRec.getRecList().put(tracksList.get(pred._1()._2).getTitle(), pred._2());
			CassandraQueryController.persist(newRec);
		}
	}
}