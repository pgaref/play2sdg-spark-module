package main.java.uk.ac.imperial.lsds.rest_client;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONObject;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class MainJerseyClient {
	
	private static List<MesosMasterStats> runntimeStats = new ArrayList<MesosMasterStats>();
	private static String outputLog = "data/experiments/mesos_stats";

	public static void main(String[] args) {
		try {
			
			SimpleDateFormat dformater = new SimpleDateFormat("#dd-M-yyyy#hh:mm:ss");
			outputLog = outputLog + dformater.format(new Date()) + ".log";
			Client client = Client.create();

			WebResource master = client
					.resource("http://wombat30.doc.res.ic.ac.uk:5050/master/stats.json");
			
			List<WebResource> slaves = new ArrayList<WebResource>();			
			slaves.add( client.resource("http://wombat26.doc.res.ic.ac.uk:5051/system/stats.json") );		
			slaves.add( client.resource("http://wombat27.doc.res.ic.ac.uk:5051/system/stats.json") );
			slaves.add( client.resource("http://wombat28.doc.res.ic.ac.uk:5051/system/stats.json") );	
			
			while (true) {
				ClientResponse response = master
						.accept("application/json").get(ClientResponse.class);

				if (response.getStatus() != 200) {
					throw new RuntimeException("Failed : HTTP error code : "
							+ response.getStatus());
				}
				MesosMasterStats curStats = new MesosMasterStats();
				String output = response.getEntity(String.class);
				JSONObject json = new JSONObject(output);

				System.out.println("Got active Slaves: "
						+ json.get("activated_slaves") + "\n"
						+ json.get("system/mem_free_bytes"));
				
				
				curStats.setActivated_slaves(Integer.parseInt(json.get("activated_slaves").toString()));
				curStats.setIdle_mem(Double.parseDouble(json.get("system/mem_free_bytes").toString()));
				
//				System.out.println("CPU: " + json.get("cpus_total") + " - "
//						+ json.get("cpus_used") + " "
//						+ json.get("cpus_percent"));
				
				curStats.setUsed_cpus(Double.parseDouble(json.get("cpus_used").toString()));
				curStats.setTotal_cpus(Double.parseDouble(json.get("cpus_total").toString()));
//				System.out.println("MEM: " + json.get("mem_total") + " - "
//						+ json.get("mem_used") + " " + json.get("mem_percent"));
				curStats.setTotal_mem(Double.parseDouble(json.get("mem_total").toString()));
				curStats.setUsed_mem( Double.parseDouble(json.get("mem_used").toString()) );
				
				
				for(WebResource tmp : slaves){
					response  = tmp.accept("application/json").get(ClientResponse.class);
					if (response.getStatus() != 200) {
						throw new RuntimeException("Failed : HTTP error code : "
								+ response.getStatus());
					}
					output = response.getEntity(String.class);
					json = new JSONObject(output);
					
					curStats.getSlaves_load_1min().add(Double.parseDouble(json.get("avg_load_1min").toString()));
					curStats.getSlaves_load_5min().add(Double.parseDouble(json.get("avg_load_5min").toString()));
				}
				
				System.out.println("Output from Server .... \n");
				System.out.println("\n ## Current Stats: "+ curStats);
			
				persistLog(curStats);
				runntimeStats.add(curStats);
				
				Thread.sleep(1000);
			}

		} catch (Exception e) {

			e.printStackTrace();

		}

	}
	
	
	private static void persistLog(MesosMasterStats stats) {

		try {
			File file = new File(outputLog);

			// if file doesnt exists, then create it
			if (!file.exists()) {
				file.getParentFile().mkdirs();
				file.createNewFile();
			}

			FileWriter fw = new FileWriter(file.getAbsoluteFile(), true);
			BufferedWriter bw = new BufferedWriter(fw);

			String newline = "\n" + stats.getTimestamp().getTime() + " "
					+ stats.getActivated_slaves()+ " "
					+ stats.getSlaves_load_1min().toString() + " " 
					+ stats.getSlaves_load_5min().toString() + " "
					+ stats.getTotal_cpus() + " " + stats.getIdle_cpus() + " " + stats.getUsed_cpus() + " " 
					+ stats.getTotal_mem() + " "+ stats.getIdle_mem() + " " + stats.getUsed_mem();
			bw.write(newline);

			bw.close();

		} catch (IOException e) {
			System.err
					.println("Writing Mesos Stats File to normal FS failed! Path : "
							+ outputLog);
			e.printStackTrace();
		}
	}

}
