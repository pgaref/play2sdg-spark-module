package main.java.uk.ac.imperial.lsds.jmeter.parser;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;

public class ParseAll {
	
	private static File file = null;
	private static Logger  logger = Logger.getLogger(ParseAll.class);
	
	public String[] getSubDirectories() {
		return file.list(new FilenameFilter() {
			@Override
			public boolean accept(File current, String name) {
				return new File(current, name).isDirectory();
			}
		});
	}
	
	public static List<File> listAllFiles(String directoryName, String fileExtension) {
        File directory = new File(directoryName);
        List<File> resultList = new ArrayList<File>();
        // get all the files from a directory
        File[] fList = directory.listFiles();
        //resultList.addAll(Arrays.asList(fList));
        for (File file : fList) {
            if (file.isFile()) {
                logger.debug("Adding File: "+ file.getAbsolutePath());
                //Avoid hidden and system files!
                if(file.getName().endsWith(fileExtension))
                	resultList.add(file);
            } else if (file.isDirectory()) {
            	logger.debug("Ignoring dir: " + file);
                //resultList.addAll(listAllFiles(file.getAbsolutePath(), fileExtension));
            }
        }      
        return resultList;
    }

	
	public static void main(String [] args) throws Exception{
		//Argument should be the path: /Users/pgaref/Documents/workspace/play2sdg-Spark-module/data/2015-11-14-13-48-48/
		// add jmeter_results for the jmeter path
		int count =1;
		int [] clientsNum = {5, 10, 50, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1200, 1500 };
		
		String path = "/Users/pgaref/Documents/workspace/play2sdg-Spark-module/data/2015-11-14-13-48-48/";
		System.out.println("# Parser Path :"+ path);
		System.out.println("# Loadsphia project: ");
		System.out.println("# \t clients \t avg(ms)	99th \t TPS \t 90th \t 75th \t cpu-util(%) \t mem-util(%) \t netIn-Mbps \t netOut-Mbps");
		
		for(int clients: clientsNum){
			System.out.print(count + "\t"+ clients +"\t\t");
			
			final String clientString = clients+"";
			File jmeterdir = new File(path+"jmeter_results");
			File[] jmeterFiles = jmeterdir.listFiles(new FilenameFilter() {
			    public boolean accept(File dir, String name) {
			        return name.matches(".+\\_"+clientString+"clients.csv");
			    }
			});
			
			if(jmeterFiles.length >1 )
				throw new Exception("More Jmeter Files matches than expected!!");
			
			JTLParser jp = new JTLParser(jmeterFiles[0].getAbsolutePath());
			jp.run();
			System.out.print(jp.getAverageLatency()+"\t\t" + jp.get99thLatency() + "\t" +String.format("%.2f",jp.getTPSAverage())+"\t"+ jp.get90thLatency() + "\t" + jp.get90thLatency() +"\t" );
			
			
			File perfdir = new File(path);
			
			File[] perfFiles = perfdir.listFiles(new FilenameFilter() {
			    public boolean accept(File dir, String name) {
			        return name.matches(".+\\_"+clientString+"clients_stats.csv");
			    }
			});
			
			ArrayList<PerformanceParser> perfParsers = new ArrayList<>();
			//parse All Perf Data
			for(File tmp  : perfFiles){
				perfParsers.add(new PerformanceParser(tmp.getAbsolutePath()));
			}
			//Summarise results
			double cpuUtil = 0;
			double memUtil = 0;
			double netinMbps = 0;
			double netoutMbps = 0;
			for(PerformanceParser pp : perfParsers){
				cpuUtil += pp.getCpuUsageAverage();
				memUtil += pp.getMemoryUsageAverage();
				netinMbps += pp.getNetInMbpsAverage();
				netoutMbps += pp.getNetOutMbpsAverage();
			}
			System.out.println(String.format("%.2f", cpuUtil) +"\t\t"+ String.format("%.2f", memUtil) +"\t\t" + String.format("%.2f", netinMbps) +"\t\t" + String.format("%.2f", netoutMbps));
			count++;
		}
		
		
	}
	
//	#  NO-HT-w16_LXC_PlaySpark-Colocated-v2
//	#  => LoadSophia file
//	#   clients     	avg(ms)	99th     TPS		    90th	75th                cpu-util(%) 	mem-util(%) \t netIn-Mbps \t netOut-Mbps
}
