/**
 * Accessor Interface Implementing Datastax Object Mapping
 * Specific interface for Track class
 * @author pgaref
 *
 */
package main.java.uk.ac.imperial.lsds.dx_controller;

import main.java.uk.ac.imperial.lsds.dx_models.Track;
import com.datastax.driver.mapping.Result;
import com.datastax.driver.mapping.annotations.Accessor;
import com.datastax.driver.mapping.annotations.Param;
import com.datastax.driver.mapping.annotations.Query;
import com.google.common.util.concurrent.ListenableFuture;


@Accessor
public interface TrackAccessorI {
	/*
	 * TODO Figure out => WHY????
	 * 
	 */
//	@Query("SELECT * FROM play_cassandra.tracks WHERE title = :title")
//	public Result<Track> getbyTitle(@Param("title") String title);
	
	@Query("SELECT * FROM play_cassandra.tracks")
	public Result<Track> getAll();
	
	@Query("SELECT * FROM play_cassandra.tracks")
	public ListenableFuture<Result<Track>> getAllAsync();
	
}
