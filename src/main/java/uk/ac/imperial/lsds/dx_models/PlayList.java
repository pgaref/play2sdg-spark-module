package main.java.uk.ac.imperial.lsds.dx_models;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.google.common.base.Objects;


@Table(keyspace = "play_cassandra", name = "playlists")
public class PlayList implements Serializable{

	@PartitionKey
	@Column(name = "id")
	public UUID id;

	@Column(name = "folder")
	public String folder;

	@Column(name = "usermail")
	public String usermail;

	@Column(name = "tracks")
	public List<String> tracks;

	public PlayList() {}
	
	public PlayList(String usermail, String folder) {
		this.id = UUID.randomUUID();
		this.folder = folder;
		this.usermail = usermail;
		this.tracks = new ArrayList<String>();
	}
	
	public PlayList(UUID id, String usermail, String folder, List<String> tracks) {
		this.id = id;
		this.folder = folder;
		this.usermail = usermail;
		this.tracks = tracks;
	}

	public void addRatingSong(String track) {
		if (tracks == null)
			tracks = new ArrayList<String>();

		tracks.add(track);
	}

	/**
	 * @return the id
	 */
	public UUID getId() {
		return id;
	}

	/**
	 * @param id
	 *            the id to set
	 */
	public void setId(UUID id) {
		this.id = id;
	}

	/**
	 * @return the folder
	 */
	public String getFolder() {
		return folder;
	}

	/**
	 * @param folder
	 *            the folder to set
	 */
	public void setFolder(String folder) {
		this.folder = folder;
	}

	/**
	 * @return the usermail
	 */
	public String getUsermail() {
		return usermail;
	}

	/**
	 * @param usermail
	 *            the usermail to set
	 */
	public void setUsermail(String usermail) {
		this.usermail = usermail;
	}

	/**
	 * @return the tracks
	 */
	public List<String> getTracks() {
		if(tracks == null)
			this.tracks = new ArrayList<String>();
		return tracks;
	}

	/**
	 * @param tracks
	 *            the tracks to set
	 */
	public void setTracks(List<String> tracks) {
		this.tracks = tracks;
	}
	
	@Override
	public int hashCode() {
		return Objects.hashCode(this.id);
	}
	

	public String toString() {

		return "\n--------------------------------------------------"
				+ "\n Playlist: " + this.folder + "\n usermail: "
				+ this.usermail + "\n Songs: "
				+ (tracks != null ? this.tracks.toString() : " empty");
	}
}