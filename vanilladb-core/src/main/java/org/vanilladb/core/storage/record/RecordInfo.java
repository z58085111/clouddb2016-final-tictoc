package org.vanilladb.core.storage.record;

import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.tx.Transaction;

public class RecordInfo implements Comparable<RecordInfo> {
	private TableInfo tblInfo;
	private RecordId recId;
	private RecordFile rf;
	
	public RecordInfo(TableInfo tblInfo, RecordId recId) {
		this.tblInfo = tblInfo;
		this.recId = recId;
	}
	
	public TableInfo tableInfo() {
	 	return tblInfo;
	}
	
	public RecordId recordId() {
		return recId;
	}
	
	public RecordFile open(Transaction tx, boolean doLog) {
		if(rf==null) {
			rf = tblInfo.open(tx, doLog);
			rf.moveToRecordId(recId);
		}
		return rf;
	}

	public void close() {
		if(rf!=null) {
			rf.close();
			rf = null;
		}
	}
	
	public boolean equals(Object obj) {
		if (obj == this)
			return true;
		if (obj == null || !(obj.getClass().equals(RecordInfo.class)))
			return false;
		RecordInfo recInfo = (RecordInfo) obj;
		return tblInfo.tableName().equals(recInfo.tblInfo.tableName()) && recId.equals(recInfo.recId);
	}
	
	public int hashCode() {
		int hash = 17;
		hash = hash * 31 + tblInfo.tableName().hashCode();
		hash = hash * 31 + recId.hashCode();
		return hash;
	}

	@Override
	public int compareTo(RecordInfo o) {
		return hashCode() - o.hashCode();
	}
}
