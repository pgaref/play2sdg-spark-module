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
	
	@Column(name = "metricsmap")
	private Map<String, String> metricsmap;
	
	public StatsTimeseries(){}
	
	public StatsTimeseries(String id){
		this.id = id;
		this.timestamp = new Date();
		this.metricsmap = new HashMap<String, String>();
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
	 * @return the metricsmap
	 */
	public Map<String, String> getMetricsmap() {
		return metricsmap;
	}

	/**
	 * @param metricsmap the metricsmap to set
	 */
	public void setMetricsmap(Map<String, String> metricsmap) {
		this.metricsmap = metricsmap;
	}


	public void collectData(SystemStats perf){
		this.getMetricsmap().put("os-name", perf.getOsName());
		this.getMetricsmap().put("cpu-vendor", perf.getCpuVendor());
		this.getMetricsmap().put("cpu-freq", perf.getCpuFreq()+"");
		this.getMetricsmap().put("cores-num", perf.getCpuCores()+"");
		this.getMetricsmap().put("system-load", perf.getSystemLoad()+"");
		this.getMetricsmap().put("system-loadavg", perf.getSystemLoadAverage() +"");
		
		int num = 0;
		for(Double val: perf.getCoresLoad()){
			this.getMetricsmap().put("core-"+num, val+"");
			num++;
		}
		
		this.getMetricsmap().put("mem-total", perf.getMemTotal()+"");
		this.getMetricsmap().put("mem-avail", perf.getMemAvailable()+"");
	}
	
	public String toString(){
		StringBuffer toret = new StringBuffer();
		for(String k :this.getMetricsmap().keySet() )
			toret.append( "K: "+ k + " V: "+ this.getMetricsmap().get(k) );
		
		return "D: "+ this.getTimestamp() +
				"ID: "+ this.getId() +
				"["+toret.toString()+"]";
	}
	
}
