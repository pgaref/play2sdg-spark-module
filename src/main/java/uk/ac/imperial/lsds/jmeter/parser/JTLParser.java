package main.java.uk.ac.imperial.lsds.jmeter.parser;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.apache.log4j.Logger;

public class JTLParser implements Runnable{
	
	//private static Logger logger = Logger.getLogger(JTLParser.class);
	private ArrayList<JTLDataPoint> dataPoints;
	private ArrayList<Long> latencyPoints;
	private String filePath;
	private int count;
	private long duration;
	
	public JTLParser(String fileFullPath){
		this.dataPoints = new ArrayList<JTLDataPoint>();
		this.latencyPoints = new ArrayList<Long>();
		this.filePath = fileFullPath;
		this.count = 0;
		this.duration=0;
	}
	
	
	private void addData(long timestamp, int responseCode, boolean success, long bytes, long activeThreads, long latency,
			int samplecount, int errorcount, String hostname, long idletime, long connecttime)  {
		JTLDataPoint toadd = new JTLDataPoint(timestamp,  responseCode,  success,  bytes,  activeThreads,  latency,
				 samplecount,  errorcount,  hostname,  idletime,  connecttime);
		this.dataPoints.add(toadd);		
	}
	/**
	 * File Format: 
	 * 0: timeStamp,
	 * 1: elapsed,
	 * 2: label
	 * 3: responseCode
	 * 4: responseMessage
	 * 5: threadName
	 * 6: dataType
	 * 7: success
	 * 8: bytes
	 * 9: grpThreads
	 * 10: allThreads
	 * 11: Latency
	 * 12: SampleCount
	 * 13: ErrorCount
	 * 14: Hostname
	 * 15: IdleTime
	 * 16: Connect
	 */
	public void run() {

		BufferedReader br = null;
		String line = "";
		String cvsSplitBy = ",";

		try {
			
			br = new BufferedReader(new FileReader(this.filePath));
			
			while( (line = br.readLine()) != null) {
				int responseCode =0;
				// use comma as separator
				String[] CsvFields = line.split(cvsSplitBy);
				
				//Always skip the header line
				if(CsvFields[0].equals("timeStamp"))
					continue;
				
						
				long timestamp = Long.parseLong(CsvFields[0]);
				try{
						responseCode = Integer.parseInt(CsvFields[3]);
				} catch (NumberFormatException ex){}
				boolean success = Boolean.getBoolean(CsvFields[7]);
				long bytes = Long.parseLong(CsvFields[8]);
				long activeThreads = Long.parseLong(CsvFields[10]);
				long latency = Long.parseLong(CsvFields[11]);
				int samplecount = Integer.parseInt(CsvFields[12]);
				int errorcount = Integer.parseInt(CsvFields[13]);
				String hostname = CsvFields[14];
				long idletime = Long.parseLong(CsvFields[15]);
				long connecttime = Long.parseLong(CsvFields[16]);
				
				addData(timestamp, responseCode, success, bytes, activeThreads, latency,
						 samplecount, errorcount, hostname, idletime, connecttime);
				this.latencyPoints.add(latency);
				count++;
			}

		} catch (FileNotFoundException e) {
			System.err.println("FileNotFoundException Exception when reading CSV file : "+ filePath);
		} catch (IOException e) {
			System.err.println("IO Exception when reading CSV file : "+ filePath);
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					System.err.println("IOException Exception when reading CSV file : "+ filePath);
				}
			}
		}

		 double tmp = this.dataPoints.get(dataPoints.size()-1).getTimestamp() - this.dataPoints.get(0).getTimestamp();
		 this.duration = Math.round((double)tmp/(double)1000);
		 //System.out.println("Done");
	}
	
	public Long get99thLatency(){
		Collections.sort(latencyPoints, new Comparator<Long>() {
			@Override
			public int compare(Long o1, Long o2) {
				return o1.compareTo(o2);
			}
		});
		return latencyPoints.get(latencyPoints.size()*99/100);	
	}
	
	public Long get90thLatency(){
		Collections.sort(latencyPoints, new Comparator<Long>() {
			@Override
			public int compare(Long o1, Long o2) {
				return o1.compareTo(o2);
			}
		});
		return latencyPoints.get(latencyPoints.size()*90/100);	
	}
	
	public Long get75thLatency(){
		Collections.sort(latencyPoints, new Comparator<Long>() {
			@Override
			public int compare(Long o1, Long o2) {
				return o1.compareTo(o2);
			}
		});
		return latencyPoints.get(latencyPoints.size()*75/100);	
	}
	
	public Long getAverageLatency(){
		long sum = 0;
		for(Long curr : latencyPoints)
			sum+=curr;
		return sum/latencyPoints.size();	
	}
	
	public Long getMinLatency(){
		Collections.sort(latencyPoints);
		return latencyPoints.get(0);
	}
	
	public Long getMaxLatency(){
		Collections.sort(latencyPoints);
		return latencyPoints.get(latencyPoints.size()-1);
	}
	
	public Long getBytesAverage(){
		long sum = 0;
		for(JTLDataPoint dp : this.dataPoints){
			sum+=dp.getBytes();
		}
		return sum/count;
	}
	
	public Double getTPSAverage(){
		return (double) count/duration;
	}
	
	
	public static void main(String[] args) {
		String jmeterPath = "/Users/pgaref/Documents/workspace/play2sdg-Spark-module/data/2015-11-14-13-48-48/jmeter_results/SummaryReport_5clients.csv" ;
		
		if(args.length == 1)
			jmeterPath = args[0];
		else if(args.length > 1 )
			System.err.println("Wrong number of arguments!!");
		
		JTLParser jp = new JTLParser(jmeterPath);
		jp.run();
		
		System.out.println("Duration "+ jp.duration + " seconds");
		System.out.println("TPS "+ jp.getTPSAverage());
		System.out.println("Bytes per sec "+ jp.getBytesAverage());
		System.out.println("Min Latency "+ jp.getMinLatency());
		System.out.println("Max Latency "+ jp.getMaxLatency());
		System.out.println("99th Latency: "+ jp.get99thLatency());
		System.out.println("90th Latency: "+ jp.get90thLatency());
		System.out.println("Average Latency: "+ jp.getAverageLatency());

	}
	
}
