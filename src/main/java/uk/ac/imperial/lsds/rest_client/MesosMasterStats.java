package main.java.uk.ac.imperial.lsds.rest_client;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class MesosMasterStats {

	
	private double total_cpus;
	private double total_mem;
	
	private double used_cpus;
	private double used_mem;
	
	private double offered_cpus;
	private double offferes_mem;
	
	private double idle_cpus;
	private double idle_mem;
	
	private int activated_slaves;
	
	private ArrayList<Double> slaves_load_1min;
	private ArrayList<Double> slaves_load_5min;
	
	private Date timestamp;
	
	public MesosMasterStats() {
		// TODO Auto-generated constructor stub
		this.timestamp = new Date();
		slaves_load_1min = new ArrayList<Double>();
		slaves_load_5min = new ArrayList<Double>();
	}
	
	public MesosMasterStats(int act_slaves) {
		this.activated_slaves = act_slaves;
		this.timestamp = new Date();
		slaves_load_1min = new ArrayList<Double>();
		slaves_load_5min = new ArrayList<Double>();
	}


	/**
	 * @return the total_cpus
	 */
	public double getTotal_cpus() {
		return total_cpus;
	}

	/**
	 * @return the total_mem
	 */
	public double getTotal_mem() {
		return total_mem;
	}

	/**
	 * @return the used_cpus
	 */
	public double getUsed_cpus() {
		return used_cpus;
	}

	/**
	 * @return the used_mem
	 */
	public double getUsed_mem() {
		return used_mem;
	}

	/**
	 * @return the offered_cpus
	 */
	public double getOffered_cpus() {
		return offered_cpus;
	}

	/**
	 * @return the offferes_mem
	 */
	public double getOffferes_mem() {
		return offferes_mem;
	}

	/**
	 * @return the idle_cpus
	 */
	public double getIdle_cpus() {
		return idle_cpus;
	}

	/**
	 * @return the idle_mem
	 */
	public double getIdle_mem() {
		return idle_mem;
	}

	/**
	 * @return the activated_slaves
	 */
	public int getActivated_slaves() {
		return activated_slaves;
	}

	/**
	 * @param total_cpus the total_cpus to set
	 */
	public void setTotal_cpus(double total_cpus) {
		this.total_cpus = total_cpus;
	}

	/**
	 * @param total_mem the total_mem to set
	 */
	public void setTotal_mem(double total_mem) {
		this.total_mem = total_mem;
	}

	/**
	 * @param used_cpus the used_cpus to set
	 */
	public void setUsed_cpus(double used_cpus) {
		this.used_cpus = used_cpus;
	}

	/**
	 * @param used_mem the used_mem to set
	 */
	public void setUsed_mem(double used_mem) {
		this.used_mem = used_mem;
	}

	/**
	 * @param offered_cpus the offered_cpus to set
	 */
	public void setOffered_cpus(double offered_cpus) {
		this.offered_cpus = offered_cpus;
	}

	/**
	 * @param offferes_mem the offferes_mem to set
	 */
	public void setOffferes_mem(double offferes_mem) {
		this.offferes_mem = offferes_mem;
	}

	/**
	 * @param idle_cpus the idle_cpus to set
	 */
	public void setIdle_cpus(double idle_cpus) {
		this.idle_cpus = idle_cpus;
	}

	/**
	 * @param idle_mem the idle_mem to set
	 */
	public void setIdle_mem(double idle_mem) {
		this.idle_mem = idle_mem;
	}

	/**
	 * @param activated_slaves the activated_slaves to set
	 */
	public void setActivated_slaves(int activated_slaves) {
		this.activated_slaves = activated_slaves;
	}
	
	/**
	 * @return the timestamp
	 */
	public Date getTimestamp() {
		return timestamp;
	}

	/**
	 * @param timestamp the timestamp to set
	 */
	public void setTimestamp(Date timestamp) {
		this.timestamp = timestamp;
	}


	/**
	 * @return the slaves_load_1min
	 */
	public ArrayList<Double> getSlaves_load_1min() {
		return slaves_load_1min;
	}

	/**
	 * @param slaves_load_1min the slaves_load_1min to set
	 */
	public void setSlaves_load_1min(ArrayList<Double> slaves_load_1min) {
		this.slaves_load_1min = slaves_load_1min;
	}

	/**
	 * @return the slaves_load_5min
	 */
	public ArrayList<Double> getSlaves_load_5min() {
		return slaves_load_5min;
	}

	/**
	 * @param slaves_load_5min the slaves_load_5min to set
	 */
	public void setSlaves_load_5min(ArrayList<Double> slaves_load_5min) {
		this.slaves_load_5min = slaves_load_5min;
	}

	@Override
	public String toString(){
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		String format = formatter.format(timestamp);
		return "#### Mesos Master Stats ###" +
				"Active slaves: " + this.activated_slaves
				+ "\nTS: " + format 
				+ "\nslaves load 1min: "+ this.slaves_load_1min.toString()
				+ "\nslaves load 5min: "+ this.slaves_load_5min.toString()
				+ "\ntotal 	cpus: "+ this.total_cpus
				+ "\nidle cpus: " + this.idle_cpus
				+ "\nused cpus: "+this.used_cpus
				
				+ "\ntotal mem: " + this.total_mem
				+ "\nidle mem: " + this.idle_mem
				+ "\nused mem: " + this.used_mem
				+ "\n-------------------------";
		
	}

}
