package org.vanilladb.core.sql;

public class TSWord {
	private long tsw;
	private long wts;
	private long delta;
	
	public TSWord (long tsw) {
		this.tsw = tsw;
		this.wts = (tsw & 0xffffffffffffL);
		this.delta = (tsw & 0x7fff000000000000L);
	}
	
	public long tsw() {
		return tsw;
	}
	
	public long wts() {
		return wts;
	}
	
	public void setWTS(long wts) {
		this.tsw = tsw & 0x000000000000L;
		this.tsw = tsw | wts;
		this.wts = wts;
	}
	
	public long rts() {
		return wts+delta;
	}
	
	public long delta() {
		return delta;
	}
}
