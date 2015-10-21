package main.java.uk.ac.imperial.lsds.dx_models;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

import main.java.uk.ac.imperial.lsds.utils.SystemStats;


@Table(keyspace="play_cassandra", name = "statseries")
public class StatsTimeseries implements Serializable{
	
	//partition key
	@PartitionKey
	@Column(name = "id")
    private String id;            
    //cluster/ remaining key
    @ClusteringColumn
    @Column(name = "timestamp")
	private java.util.Date timestamp;
	
	@Column(name = "metricsMap")
	private Map<String, String> metricsMap;
	
	public StatsTimeseries(){}
	
	public StatsTimeseries(String id){
		this.id = id;
		this.timestamp = new Date();
		this.metricsMap = new HashMap<String, String>();
	}
	
	/**
	 * @return the id
	 */
	public String getId() {
		return id;
	}

	/**
	 * @param id the id to set
	 */
	public void setId(String id) {
		this.id = id;
	}

	/**
	 * @return the timestamp
	 */
	public java.util.Date getTimestamp() {
		return timestamp;
	}

	/**
	 * @param timestamp the timestamp to set
	 */
	public void setTimestamp(java.util.Date timestamp) {
		this.timestamp = timestamp;
	}

	/**
	 * @return the metricsMap
	 */
	public Map<String, String> getMetricsMap() {
		return metricsMap;
	}

	/**
	 * @param metricsMap the metricsMap to set
	 */
	public void setMetricsMap(Map<String, String> metricsMap) {
		this.metricsMap = metricsMap;
	}


	public void collectData(SystemStats perf){
		this.getMetricsMap().put("os-name", perf.getOsName());
		this.getMetricsMap().put("cpu-vendor", perf.getCpuVendor());
		this.getMetricsMap().put("cpu-freq", perf.getCpuFreq()+"");
		this.getMetricsMap().put("cores-num", perf.getCpuCores()+"");
		this.getMetricsMap().put("system-load", perf.getSystemLoad()+"");
		this.getMetricsMap().put("system-loadavg", perf.getSystemLoadAverage() +"");
		
		int num = 0;
		for(Double val: perf.getCoresLoad()){
			this.getMetricsMap().put("core-"+num, val+"");
			num++;
		}
		
		this.getMetricsMap().put("mem-total", perf.getMemTotal()+"");
		this.getMetricsMap().put("mem-avail", perf.getMemAvailable()+"");
	}
	
	public String toString(){
		StringBuffer toret = new StringBuffer();
		for(String k :this.getMetricsMap().keySet() )
			toret.append( "K: "+ k + " V: "+ this.getMetricsMap().get(k) );
		
		return "D: "+ this.getTimestamp() +
				"ID: "+ this.getId() +
				"["+toret.toString()+"]";
	}



}
