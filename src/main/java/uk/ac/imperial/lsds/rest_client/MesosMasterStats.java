package main.java.uk.ac.imperial.lsds.rest_client;

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
	
	public MesosMasterStats() {
		// TODO Auto-generated constructor stub
	}
	
	public MesosMasterStats(int act_slaves) {
		this.activated_slaves = act_slaves;
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
	
	@Override
	public String toString(){
		return "#### Mesos Master Stats ###"
				+ "Active Slaves: "+ this.activated_slaves;
	}

}
