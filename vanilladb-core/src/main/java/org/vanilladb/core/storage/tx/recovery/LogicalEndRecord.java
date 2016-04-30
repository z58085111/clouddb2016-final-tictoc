package org.vanilladb.core.storage.tx.recovery;

import org.vanilladb.core.storage.log.LogSeqNum;

public abstract class LogicalEndRecord {
	protected LogSeqNum logicalStartLSN;

	public LogSeqNum getlogicalStartLSN(){
		return logicalStartLSN;
	}
}
