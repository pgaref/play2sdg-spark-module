package main.java.uk.ac.imperial.lsds.rest_client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.json.JSONArray;
import org.json.JSONObject;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.core.PackagesResourceConfig;
import com.sun.jersey.api.core.ResourceConfig;
import com.sun.jersey.api.json.JSONConfiguration;

public class MesosScaleTimer {
	
	private static int instancesNo;
	private static String hostName;
	private static String appName;
	private static Client client;
	
	private static long deployment_start;
	private static long deployment_end;
	
	
	public MesosScaleTimer(int instances, String marathon, String appName ){
		MesosScaleTimer.instancesNo = instances;
		MesosScaleTimer.hostName = marathon;
		MesosScaleTimer.appName=appName;
		MesosScaleTimer.client = Client.create();

	}
	
	public void checkRunning(ArrayList<String> instances) {
		
		while(instances.size()!= instancesNo)
			instances = this.getTasks();
		
		deployment_end = System.currentTimeMillis();
		System.out.println("Deployment done ready after: "+ (deployment_end-deployment_start) +" ms");

		long start = System.currentTimeMillis();
		int validCount = 0;
		while (validCount != instancesNo) {
			for (String instance : instances) {
				WebResource master = client
				// .resource("http://wombat27.doc.res.ic.ac.uk:53431");
						.resource("http://" + instance);
				try {
					ClientResponse response = master
							.accept(MediaType.TEXT_HTML).get(
									ClientResponse.class);

					if (response.getStatus() != 200) {
						System.out.println("Failed : HTTP error code : "
								+ response.getStatus());
						validCount = 0;
						break;
						// throw new
						// RuntimeException("Failed : HTTP error code : "
						// + response.getStatus());
					} else {
						//System.out.println("Host (" +instance+") - OK !");
						validCount++;
					}
				} catch (com.sun.jersey.api.client.ClientHandlerException ex) {
					//System.out.println("Host (" +instance+") "+ " Connection problem:  "+ ex.getMessage());
					validCount = 0;
					break;
				}
				// System.out.println("Got responce " + response);
			}
		}
		long end = System.currentTimeMillis();
		System.out.println("Instances Ready after : "+ (end-start) + " ms");

	}
	
	public void scaleOut() {
		
		deployment_start = System.currentTimeMillis();
	    
		ClientConfig clientConfig = new DefaultClientConfig();              
		clientConfig.getFeatures().put(JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);     
		client = Client.create(clientConfig);
		try {
			
			Map<String,Object> postBody = new HashMap<String,Object>();
			postBody.put("id", appName);
			postBody.put("instances", instancesNo);
			
			WebResource master = client.resource(hostName+"/v1/apps/scale");
			ClientResponse response = master.accept("application/json")
			                .type("application/json").post(ClientResponse.class, postBody);

//			if (response.getStatus() != 201) {
//				throw new RuntimeException("Failed : HTTP error code : "
//						+ response.getStatus());
//			}

			System.out.println("Scale out POST sent to Mesos Server .... \n");
//			String output = response.getEntity(String.class);
//			System.out.println(output);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public ArrayList<String> getTasks(){
		
		WebResource master = client.resource(hostName+"/v1/tasks");
		ClientResponse response = master
				.accept("application/json").get(ClientResponse.class);

		if (response.getStatus() != 200) {
			throw new RuntimeException("Failed : HTTP error code : "
					+ response.getStatus());
		}
		ArrayList<String> allinstances = new ArrayList<String>();
		
		String output = response.getEntity(String.class);
		JSONObject json = new JSONObject(output);
		
//		System.out.println("Received: "+ json.toString());
		try{
			JSONArray app = json.getJSONArray(appName);
			//System.out.println("App Tasks: "+ app);	
			for(int i =0; i <  app.length(); i ++){
				JSONObject task  = app.getJSONObject(i);
				
//				System.out.println("Host: "+ task.get("host") );
//				System.out.println("Ports: "+ task.getJSONArray("ports") );
				
				JSONArray allports =  task.getJSONArray("ports");
				for(int j= 0; j < allports.length(); j++)
					allinstances.add(task.get("host")+ ":"+allports.getInt(j));
			}
		}catch(org.json.JSONException e){
			System.out.println("Application " + appName + " not found!");
		}finally{
			//System.out.println("Done parsing json responce");
			return allinstances;
		}
		
	}
	
	
	public static void main(String[] args) {
		
		long total_start = System.currentTimeMillis();
		
		MesosScaleTimer timer = new MesosScaleTimer(9, "http://wombat30.doc.res.ic.ac.uk:8080", "play_isolated_2G");
		timer.scaleOut();
		ArrayList<String> allinstances = timer.getTasks();
		timer.checkRunning(allinstances);
		
		System.out.println("Total job took: "+ (System.currentTimeMillis()-total_start) + " ms" );
		
	}

}
