package org.vanilladb.core.storage.tx.recovery;

import org.vanilladb.core.storage.log.LogSeqNum;

public interface CompesationLogRecord {
	
	public LogSeqNum getUndoNextLSN();

}
