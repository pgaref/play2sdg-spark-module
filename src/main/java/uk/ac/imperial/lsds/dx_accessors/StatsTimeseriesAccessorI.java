package main.java.uk.ac.imperial.lsds.dx_accessors;


import main.java.uk.ac.imperial.lsds.dx_models.StatsTimeseries;

import com.datastax.driver.mapping.Result;
import com.datastax.driver.mapping.annotations.Accessor;
import com.datastax.driver.mapping.annotations.Query;
import com.google.common.util.concurrent.ListenableFuture;

@Accessor
public interface StatsTimeseriesAccessorI {

	@Query("SELECT * FROM play_cassandra.statseries ")
	public Result<StatsTimeseries> getAll();

	@Query("SELECT * FROM play_cassandra.statseries ")
	public ListenableFuture<Result<StatsTimeseries>> getAllAsync();
}
