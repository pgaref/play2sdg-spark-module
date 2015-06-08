package main.java.uk.ac.imperial.lsds.stats;

import org.json.JSONObject;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
 

public class MainJerseyClient {

	  public static void main(String[] args) {
			try {
		 
				Client client = Client.create();
		 
				WebResource webResource = client
				   .resource("http://wombat30.doc.res.ic.ac.uk:5050/master/stats.json");
		 
				ClientResponse response = webResource.accept("application/json")
		                   .get(ClientResponse.class);
		 
				if (response.getStatus() != 200) {
				   throw new RuntimeException("Failed : HTTP error code : "
					+ response.getStatus());
				}
		 
				String output = response.getEntity(String.class);
				JSONObject json = new JSONObject(output);
				
				System.out.println("Got active Slaves: " + json.get("activated_slaves")  +"\n"+ json.get("system/mem_free_bytes"));
				System.out.println("CPU: "+ json.get("cpus_total") + " - " + json.get("cpus_used") + " " +json.get("cpus_percent"));
				System.out.println("MEM: "+ json.get("mem_total") + " - " + json.get("mem_used") + " " +json.get("mem_percent"));
		 
				System.out.println("Output from Server .... \n");
				System.out.println(output);
		 
			  } catch (Exception e) {
		 
				e.printStackTrace();
		 
			  }
		 
			}
	  

}
