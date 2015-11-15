package main.java.uk.ac.imperial.lsds.jmeter.parser;

public class PerformanceDataPoint {

	private double total_cpu_precent;
	private double memory_usage_precent;
	private double in_throughput_mbps;
	private double out_throughput_mbps;
	private double write_speed_mbps;
	private double read_speed_mbps;
	private double iowait_precent;
	
	public PerformanceDataPoint(double total_cpu_precent, double memory_usage_precent, double in_throughput_mbps,
			double out_throughput_mbps, double write_speed_mbps, double read_speed_mbps, double iowait_precent) {
		super();
		this.total_cpu_precent = total_cpu_precent;
		this.memory_usage_precent = memory_usage_precent;
		this.in_throughput_mbps = in_throughput_mbps;
		this.out_throughput_mbps = out_throughput_mbps;
		this.write_speed_mbps = write_speed_mbps;
		this.read_speed_mbps = read_speed_mbps;
		this.iowait_precent = iowait_precent;
	}
	
	
	public double getTotal_cpu_precent() {
		return total_cpu_precent;
	}
	public void setTotal_cpu_precent(double total_cpu_precent) {
		this.total_cpu_precent = total_cpu_precent;
	}
	public double getMemory_usage_precent() {
		return memory_usage_precent;
	}
	public void setMemory_usage_precent(double memory_usage_precent) {
		this.memory_usage_precent = memory_usage_precent;
	}
	public double getIn_throughput_mbps() {
		return in_throughput_mbps;
	}
	public void setIn_throughput_mbps(double in_throughput_mbps) {
		this.in_throughput_mbps = in_throughput_mbps;
	}
	public double getOut_throughput_mbps() {
		return out_throughput_mbps;
	}
	public void setOut_throughput_mbps(double out_throughput_mbps) {
		this.out_throughput_mbps = out_throughput_mbps;
	}
	public double getWrite_speed_mbps() {
		return write_speed_mbps;
	}
	public void setWrite_speed_mbps(double write_speed_mbps) {
		this.write_speed_mbps = write_speed_mbps;
	}
	public double getRead_speed_mbps() {
		return read_speed_mbps;
	}
	public void setRead_speed_mbps(double read_speed_mbps) {
		this.read_speed_mbps = read_speed_mbps;
	}
	public double getIowait_precent() {
		return iowait_precent;
	}
	public void setIowait_precent(double iowait_precent) {
		this.iowait_precent = iowait_precent;
	}
	
	
}
