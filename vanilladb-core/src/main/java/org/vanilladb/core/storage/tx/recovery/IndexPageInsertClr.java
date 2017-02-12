package org.vanilladb.core.storage.tx.recovery;

import static org.vanilladb.core.sql.Type.BIGINT;

import java.util.List;

import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.storage.log.BasicLogRecord;
import org.vanilladb.core.storage.log.LogSeqNum;
import org.vanilladb.core.storage.tx.Transaction;

public class IndexPageInsertClr extends IndexPageInsertRecord implements CompesationLogRecord {
	private LogSeqNum undoNextLSN;

	public IndexPageInsertClr(long txNum, String indexName, boolean isDirPage, Type keyType, long blkNum, int slotId,
			LogSeqNum undoNextLSN) {
		super(txNum, indexName, isDirPage, keyType, blkNum, slotId);
		this.undoNextLSN = undoNextLSN;

	}

	public IndexPageInsertClr(BasicLogRecord rec) {
		super(rec);
		undoNextLSN = new LogSeqNum((Long) rec.nextVal(BIGINT).asJavaVal(), (Long) rec.nextVal(BIGINT).asJavaVal());
	}

	@Override
	public int op() {
		return OP_INDEX_PAGE_INSERT_CLR;
	}

	/**
	 * Does nothing, because compensation log record is redo-Only
	 */
	@Override
	public void undo(Transaction tx) {
		// do nothing

	}

	@Override
	public LogSeqNum getUndoNextLSN() {
		return undoNextLSN;
	}

	@Override
	public String toString() {
		String str = super.toString();
		return str.substring(0, str.length() - 1) + " " + undoNextLSN + ">";
	}

	@Override
	public List<Constant> buildRecord() {
		List<Constant> rec = super.buildRecord();
		rec.set(0, new IntegerConstant(op()));
		rec.add(new BigIntConstant(undoNextLSN.blkNum()));
		rec.add(new BigIntConstant(undoNextLSN.offset()));
		return rec;
	}

}
