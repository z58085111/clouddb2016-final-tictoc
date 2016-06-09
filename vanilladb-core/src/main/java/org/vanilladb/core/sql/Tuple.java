package org.vanilladb.core.sql;

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
}
