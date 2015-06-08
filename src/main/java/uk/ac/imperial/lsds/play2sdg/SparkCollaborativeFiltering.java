package main.java.uk.ac.imperial.lsds.play2sdg;

import java.util.List;
import java.util.logging.Logger;

import scala.Tuple2;
import main.java.uk.ac.imperial.lsds.models.Track;
import main.java.uk.ac.imperial.lsds.utils.LastFMDataParser;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.SparkConf;

public class SparkCollaborativeFiltering {

	private static Logger logger = Logger.getLogger("main.java.uk.ac.imperial.lsds.play2sdg.SparkCollaborativeFiltering");
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().set("spark.executor.memory", "3g")
				.setMaster("local")
				.setAppName("Collaborative Filtering Example");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		/*
		 * First Fetch the Track List 
		 */
		LastFMDataParser parser = new LastFMDataParser( "hdfs://wombat30.doc.res.ic.ac.uk:8020/user/pg1712/lastfm_subset");
		List<Track> tracksList = parser.parseDataSet(false);
		System.out.println("## Fetched # "+ tracksList.size() +" Tracks ##");
		
		// Load and parse the data
		//String path = "data/mllib/als/test.data";
		String path = "hdfs://wombat30.doc.res.ic.ac.uk:8020/user/pg1712/test.data";
				
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

		// model.save("myModelPath");
		// MatrixFactorizationModel sameModel =
		// MatrixFactorizationModel.load("myModelPath");
	}
}