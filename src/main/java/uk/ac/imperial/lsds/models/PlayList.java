package main.java.uk.ac.imperial.lsds.models;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import javax.persistence.Column;
import javax.persistence.ElementCollection;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import com.impetus.kundera.index.Index;
import com.impetus.kundera.index.IndexCollection;

//@Security.Authenticated(Secured.class)
@Entity
@Table(name = "playlists", schema = "play_cassandra@cassandra_pu")
// Secondary index
@IndexCollection(columns = { @Index(name = "usermail") })
public class PlayList implements Serializable {

	private static final long serialVersionUID = 4L;

	@Id
	public UUID id;

	@Column(name = "folder")
	public String folder;

	@Column(name = "usermail")
	public String usermail;

	@ElementCollection
	@Column(name = "tracks")
	public List<String> tracks;

	public PlayList() {
	}

	public PlayList(String usermail, String fname) {

		this.id = UUID.randomUUID();
		this.folder = fname;
		this.usermail = usermail;
		this.tracks = new ArrayList<String>();

	}

	public void addRatingSong(Track s) {
		if (tracks == null)
			tracks = new ArrayList<String>();

		tracks.add(s.getTitle());
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

	public String toString() {

		return "\n--------------------------------------------------"
				+ "\n Playlist: " + this.folder + "\n usermail: "
				+ this.usermail + "\n Songs: "
				+ (tracks != null ? this.tracks.toString() : " empty");
	}
}