package main.java.uk.ac.imperial.lsds.models;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;


public class PlayList implements Serializable{
	
    public UUID id;
    public String folder;
    public String usermail;
    public List<String> titles = new ArrayList<String>();
    
    public PlayList(){
    	
    }
    
    
    public PlayList(String usermail, String fname) {
    	
    	this.id = UUID.randomUUID();
    	this.folder = fname;
    	this.usermail= usermail;
    	
    }
    
    public void addRatingSong(Track s){
    	this.titles.add(s.getTitle());
    }

	/**
	 * @return the id
	 */
	public UUID getId() {
		return id;
	}

	/**
	 * @param id the id to set
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
	 * @param folder the folder to set
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
	 * @param usermail the usermail to set
	 */
	public void setUsermail(String usermail) {
		this.usermail = usermail;
	}

	/**
	 * @return the songs
	 */
	public List<String> getSongs() {
		return titles;
	}

	/**
	 * @param songs the songs to set
	 */
	public void setSongs(List<String> songs) {
		this.titles = songs;
	}
	
	/**
	 * @return the titles
	 */
	public List<String> getTitles() {
		return titles;
	}


	/**
	 * @param titles the titles to set
	 */
	public void setTitles(List<String> titles) {
		this.titles = titles;
	}

	
	public String toString(){
		return "\n--------------------------------------------------"
				+ "\n Playlist: "+this.folder
				+"\n usermail: "+ this.usermail 
				+"\n Songs: "+ this.titles.toString();
	}


}