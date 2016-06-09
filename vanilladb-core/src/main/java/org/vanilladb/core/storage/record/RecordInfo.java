package org.vanilladb.core.storage.record;

import org.vanilladb.core.storage.metadata.TableInfo;
import org.vanilladb.core.storage.tx.Transaction;

public class RecordInfo {
	private TableInfo tblInfo;
	private RecordId recId;
	private RecordFile rf;
	
	public RecordInfo(TableInfo tblInfo, RecordId recId) {
		this.tblInfo = tblInfo;
		this.recId = recId;
	}
	
	public RecordFile open(Transaction tx) {
		if(rf==null) {
			rf = tblInfo.open(tx, false);
			rf.moveToRecordId(recId);
		}
		return rf;
	}

	public void close() {
		if(rf!=null)
			rf.close();
	}
}
