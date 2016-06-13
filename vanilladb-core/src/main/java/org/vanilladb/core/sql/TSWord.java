package org.vanilladb.core.sql;

public class TSWord {
	public static final long WTS_MASK   = 0x0000ffffffffffffL;
	public static final long LOCK_MASK	= 0x8000000000000000L;
	public static final long DELTA_MASK = 0x7fff000000000000L;
	private long tsw;
	
	public TSWord (long tsw) {
		this.tsw = tsw;
	}
	
	public long tsw() {
		return tsw;
	}
	
	public long wts() {
		return (tsw & WTS_MASK);
	}
	
	public long rts() {
		return wts()+delta();
	}
	
	public long delta() {
		return (tsw & DELTA_MASK) >>> 48;
	}
	
	public boolean equals(Object obj) {
		if (obj == this)
			return true;
		if (obj == null || !(obj.getClass().equals(TSWord.class)))
			return false;
		TSWord t = (TSWord) obj;
		return tsw == t.tsw;
	}
	
}
