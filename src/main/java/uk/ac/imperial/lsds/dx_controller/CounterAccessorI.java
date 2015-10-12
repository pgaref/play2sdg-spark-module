/**
 * Accessor Interface Implementing Datastax Object Mapping
 * Specific interface for Counter class
 * @author pgaref
 *
 */
package main.java.uk.ac.imperial.lsds.dx_controller;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.mapping.Result;
import com.datastax.driver.mapping.annotations.Accessor;
import com.datastax.driver.mapping.annotations.Param;
import com.datastax.driver.mapping.annotations.Query;
import com.google.common.util.concurrent.ListenableFuture;

import main.java.uk.ac.imperial.lsds.dx_models.Counter;

@Accessor
public interface CounterAccessorI {
	/*
	 * 	UPDATE counterks.page_view_counts
 	 * 	SET counter_value = counter_value + 1
 	 *  WHERE url_name='counters
	 */
	@Query("UPDATE play_cassandra.counters SET counter = counter + 1 WHERE key = :key")
	public ResultSet incrementCounter(@Param("key") String key);
	
	@Query("UPDATE play_cassandra.counters SET counter = counter - 1 WHERE key = :key")
	public ResultSet decrementCounter(@Param("key") String key);
	
    @Query("SELECT * FROM play_cassandra.counters")
    public Result<Counter> getAll();

    @Query("SELECT * FROM play_cassandra.counters")
    public ListenableFuture<Result<Counter>> getAllAsync();

}
