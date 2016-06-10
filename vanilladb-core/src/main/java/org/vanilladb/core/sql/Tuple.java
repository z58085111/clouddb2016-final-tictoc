package org.vanilladb.core.sql;

import java.util.LinkedHashMap;
import java.util.Map;

import org.vanilladb.core.storage.record.RecordFile;
import org.vanilladb.core.storage.record.RecordInfo;
import org.vanilladb.core.storage.tx.Transaction;

public class Tuple {
	private RecordInfo recInfo;
	private Map<String, Constant> recVal;
	private TS_word tsw;
	
	public Tuple(RecordInfo recInfo, TS_word tsw) {
		this.recInfo = recInfo;
		this.tsw = tsw;
		this.recVal = new LinkedHashMap<String, Constant>();
	}
	
	public RecordFile openCurrentTuple(Transaction tx) {
		return recInfo.open(tx);
	}
	
	public void closeCurrentTuple() {
		recInfo.close();
	}
	
	public TS_word getTS_WORD() {
		return tsw;
	}
	
	public Constant getVal(String fld) {
		return recVal.get(fld);
	}
	
	public void setVal(String fld, Constant val) {
		recVal.put(fld, val);
	}
	
	public RecordInfo recordInfo() {
		return recInfo;
	}
	
	public boolean equals(Object obj) {
		if(!(obj instanceof Tuple))
			return false;
		if(obj == this)
			return true;
		Tuple t = (Tuple) obj;
		return recInfo.equals(t.recInfo);
	}
}
