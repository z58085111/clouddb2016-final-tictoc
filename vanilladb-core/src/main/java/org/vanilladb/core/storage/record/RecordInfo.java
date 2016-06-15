package org.vanilladb.core.storage.record;

public class RecordInfo implements Comparable<RecordInfo> {
	private RecordId recId;
	private RecordFile rf;
	
	public RecordInfo(RecordFile rf, RecordId recId) {
		this.rf = rf;
		this.recId = recId;
	}
	
	public RecordId recordId() {
		return recId;
	}
	
	public RecordFile open() {
		if(!rf.currentRecordId().equals(recId))
			rf.moveToRecordId(recId);
		return rf;
	}

	public void close() {
		rf.close();
	}
	
	public boolean equals(Object obj) {
		if (obj == this)
			return true;
		if (obj == null || !(obj.getClass().equals(RecordInfo.class)))
			return false;
		RecordInfo recInfo = (RecordInfo) obj;
		return recId.equals(recInfo.recId);
	}
	
	public int hashCode() {
		int hash = 17;
		hash = hash * 31 + recId.hashCode();
		return hash;
	}

	@Override
	public int compareTo(RecordInfo o) {
		return hashCode() - o.hashCode();
	}
}
