package org.vanilladb.core.sql;

public class TS_word {
	private long tsw;
	private long delta;
	private long wts;
	
	public TS_word (long tsw) {
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
	
	public long rts() {
		return wts+delta;
	}
	
	public long delta() {
		return delta;
	}
}
