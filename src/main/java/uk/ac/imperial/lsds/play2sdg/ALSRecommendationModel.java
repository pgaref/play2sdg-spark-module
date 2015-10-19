package main.java.uk.ac.imperial.lsds.play2sdg;

import java.io.Serializable;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;

import scala.Tuple2;



public class ALSRecommendationModel implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 101L;
	static Logger logger = Logger.getLogger(ALSRecommendationModel.class);
	private double MSE;
	private long predictionsSize;
	
	
	public ALSRecommendationModel() {
        this.MSE = 0;
        this.predictionsSize = 1L;
    }
	
	public JavaPairRDD<Tuple2<Integer, Integer>, Double> runModel(JavaRDD<Rating> ratings){
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
		
		this.predictionsSize = predictions.toArray().size();
		this.MSE = MSE;
		
		logger.info("\n ## Rates and Predictions Size: "+ predictionsSize);
		logger.info("\n ## Mean Squared Error = " + String.format("%2f", MSE));
		
		return predictions;
	}

	/**
	 * @return the mSE
	 */
	public double getMSE() {
		return MSE;
	}

	/**
	 * @param mSE the mSE to set
	 */
	public void setMSE(double mSE) {
		MSE = mSE;
	}

	/**
	 * @return the predictionsSize
	 */
	public long getPredictionsSize() {
		return predictionsSize;
	}

	/**
	 * @param predictionsSize the predictionsSize to set
	 */
	public void setPredictionsSize(long predictionsSize) {
		this.predictionsSize = predictionsSize;
	}

}
