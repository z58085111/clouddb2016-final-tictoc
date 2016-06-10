package org.vanilladb.core.sql;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.vanilladb.core.storage.record.RecordFile;
import org.vanilladb.core.storage.record.RecordInfo;
import org.vanilladb.core.storage.tx.Transaction;

public class Tuple {
	private TupleType type;
	private RecordInfo recInfo;
	private Map<String, Constant> recVal;
	private TS_word tsw;
	
	public Tuple(TupleType type, RecordInfo recInfo, TS_word tsw) {
		this.type = type;
		this.recInfo = recInfo;
		this.tsw = tsw;
		this.recVal = new LinkedHashMap<String, Constant>();
	}
	
	public TupleType type() {
		return type;
	}
	
	public RecordFile openCurrentTuple(Transaction tx, boolean doLog) {
		return recInfo.open(tx, doLog);
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
	
	public void executeUpdate(Transaction tx) {
		RecordFile rf=null;
		switch(type) {
		case MODIFY:
		case INSERT:
			rf = openCurrentTuple(tx, true);
//			rf.insert();
			for(Entry<String, Constant> entry : recVal.entrySet()) {
				rf.setVal(entry.getKey(), entry.getValue());
			}
			closeCurrentTuple();
			break;
		case DELETE:
			rf = openCurrentTuple(tx, true);
			rf.delete();
			closeCurrentTuple();
			break;
		default:
			break;
		}
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
