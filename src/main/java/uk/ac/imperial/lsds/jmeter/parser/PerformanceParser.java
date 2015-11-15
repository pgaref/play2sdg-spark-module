package main.java.uk.ac.imperial.lsds.jmeter.parser;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.log4j.Logger;

public class PerformanceParser implements Runnable{
	
	private static Logger logger = Logger.getLogger(PerformanceParser.class);
	private ArrayList<PerformanceDataPoint> datapoints;
	private String filePath;
	private int count;
	private long duration;
	
	public PerformanceParser(String filenamePath){
		this.filePath = filenamePath;
		this.datapoints = new ArrayList<PerformanceDataPoint>();
		this.count = 0 ;
		this.run();
	}
	
	private void addData(double total_cpu_precent, double memory_usage_precent, double in_throughput_mbps,
			double out_throughput_mbps, double write_speed_mbps, double read_speed_mbps, double iowait_precent){	
		
		PerformanceDataPoint dp = new PerformanceDataPoint( total_cpu_precent,  memory_usage_precent,  in_throughput_mbps,
				 out_throughput_mbps,  write_speed_mbps,  read_speed_mbps,  iowait_precent);
		this.datapoints.add(dp);
	}
	
	/**
	 * 0: Total_CPU_usage_percent,
	 * 1: Memory_usage_percent,
	 * 2: Throughput_in_Mbps,
	 * 3:Throughput_out_Mbps,
	 * 4: Total_CPU_usage_iowait_percent,
	 * 5: Core-0_usage_percent, 
	 * 6: Core-1_usage_percent,
	 * 7:Core-2_usage_percent,
	 * 8: Core-3_usage_percent,
	 * ....
	 * len-3: Read_speed_MBps,
	 * len-2: Write_speed_MBps,
	 * len-1: Avg_CPU_usage
	 */
	@Override
	public void run(){
		BufferedReader br = null;
		
		try{
			br = new BufferedReader(new FileReader(this.filePath));
			String line = null;
			String delimiter = ",";
			
			while( (line = br.readLine()) != null){
				//Always skip the first line
				if(count == 0){
					count++;
					continue;
				}
				String [] values = line.split(delimiter);
				
				addData(Double.parseDouble(values[0]), Double.parseDouble(values[1]), Double.parseDouble(values[2]), 
						Double.parseDouble(values[3]), Double.parseDouble(values[values.length-2]),Double.parseDouble(values[values.length-3]), Double.parseDouble(values[4]));
			}
			duration = count-1;
			
		} catch (FileNotFoundException e) {
			logger.error("FileNotFoundException Exception when reading CSV file : "+ filePath);
		} catch (IOException e) {
			logger.error("IO Exception when reading CSV file : "+ filePath);
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					logger.error("IOException Exception when reading CSV file : "+ filePath);
				}
			}
		}
	}
	
	public double getCpuUsageAverage(){
		double sum = 0;
		for(PerformanceDataPoint pp : this.datapoints){
			sum+=pp.getTotal_cpu_precent();
		}
		return sum/this.datapoints.size();
	}
	
	public double getMemoryUsageAverage(){
		double sum = 0;
		for(PerformanceDataPoint pp : this.datapoints){
			sum+=pp.getMemory_usage_precent();
		}
		return sum/this.datapoints.size();
	}
	
	public double getNetInMbpsAverage(){
		double sum = 0;
		for(PerformanceDataPoint pp : this.datapoints){
			sum+=pp.getIn_throughput_mbps();
		}
		return sum/this.datapoints.size();
	}
	
	public double getNetOutMbpsAverage(){
		double sum = 0;
		for(PerformanceDataPoint pp : this.datapoints){
			sum+=pp.getOut_throughput_mbps();
		}
		return sum/this.datapoints.size();
	}
	
	public double getIOwaitAverage(){
		double sum = 0;
		for(PerformanceDataPoint pp : this.datapoints){
			sum+=pp.getIowait_precent();
		}
		return sum/this.datapoints.size();
	}
	
	
	
	public static void main(String[] args) {
		String filePath = "/Users/pgaref/Documents/workspace/play2sdg-Spark-module/data/2015-11-14-13-48-48/wombat16_5clients_stats.csv" ;
		
		if(args.length == 1)
			filePath = args[0];
		else if(args.length > 1 )
			System.err.println("Wrong number of arguments!!");
		
		PerformanceParser pp = new PerformanceParser(filePath);
		pp.run();
		
		System.out.println("CPU avg: "+ pp.getCpuUsageAverage());
		System.out.println("Mem avg: "+ pp.getMemoryUsageAverage());
		System.out.println("Net In Mbps: "+ pp.getNetInMbpsAverage());
		System.out.println("Net Out Mbps: "+ pp.getNetOutMbpsAverage());
		System.out.println("IOWait avg: "+ pp.getIOwaitAverage());
		
	}

}
