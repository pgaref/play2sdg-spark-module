package main.java.uk.ac.imperial.lsds.dx_controller;

import main.java.uk.ac.imperial.lsds.dx_models.Track;

import com.datastax.driver.mapping.Result;
import com.datastax.driver.mapping.annotations.Accessor;
import com.datastax.driver.mapping.annotations.Query;
import com.google.common.util.concurrent.ListenableFuture;

@Accessor
public interface TrackAccessorI {

	
	@Query("SELECT * FROM play_cassandra.tracks")
	public Result<Track> getAll();
	
	@Query("SELECT * FROM play_cassandra.tracks")
	public ListenableFuture<Result<Track>> getAllAsync();
	
}
