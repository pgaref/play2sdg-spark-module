package main.java.uk.ac.imperial.lsds.jmeter.parser;

public class JTLDataPoint {

	private long timestamp;
	private int responseCode;
	private boolean success;
	private long bytes;
	private long activeThreads;
	private long latency;
	private int samplecount;
	private int errorcount;
	private String hostname;
	private long idletime;
	private long connecttime;

	public JTLDataPoint(long timestamp, int responseCode, boolean success, long bytes, long activeThreads, long latency,
			int samplecount, int errorcount, String hostname, long idletime, long connecttime) {
		this.timestamp = timestamp;
		this.responseCode = responseCode;
		this.success = success;
		this.bytes = bytes;
		this.activeThreads = activeThreads;
		this.latency = latency;
		this.samplecount = samplecount;
		this.errorcount = errorcount;
		this.hostname = hostname;
		this.idletime = idletime;
		this.connecttime = connecttime;

	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public int getResponseCode() {
		return responseCode;
	}

	public void setResponseCode(int responseCode) {
		this.responseCode = responseCode;
	}

	public boolean isSuccess() {
		return success;
	}

	public void setSuccess(boolean success) {
		this.success = success;
	}

	public long getBytes() {
		return bytes;
	}

	public void setBytes(long bytes) {
		this.bytes = bytes;
	}

	public long getActiveThreads() {
		return activeThreads;
	}

	public void setActiveThreads(long activeThreads) {
		this.activeThreads = activeThreads;
	}

	public long getLatency() {
		return latency;
	}

	public void setLatency(long latency) {
		this.latency = latency;
	}

	public int getSamplecount() {
		return samplecount;
	}

	public void setSamplecount(int samplecount) {
		this.samplecount = samplecount;
	}

	public int getErrorcount() {
		return errorcount;
	}

	public void setErrorcount(int errorcount) {
		this.errorcount = errorcount;
	}

	public String getHostname() {
		return hostname;
	}

	public void setHostname(String hostname) {
		this.hostname = hostname;
	}

	public long getIdletime() {
		return idletime;
	}

	public void setIdletime(long idletime) {
		this.idletime = idletime;
	}

	public long getConnecttime() {
		return connecttime;
	}

	public void setConnecttime(long connecttime) {
		this.connecttime = connecttime;
	}

	public String toString() {
		return "[TS: " + this.timestamp + "]\n" + "[ResponceCode: " + this.responseCode + "]\n" + "[Success: "
				+ this.success + "]\n" + "[Bytes: " + this.bytes + "]\n" + "[Active Theads" + this.activeThreads + "]\n"
				+ "[Latency: " + this.latency + "]\n" + "[SampeCount: " + this.samplecount + "]\n" + "[ErrorCount: "
				+ this.errorcount + "]\n" + "[Hostname: " + this.hostname + "]\n" + "[IdleTime" + this.idletime + "]\n"
				+ "[ConnectTime" + this.connecttime + "]";
	}

}
