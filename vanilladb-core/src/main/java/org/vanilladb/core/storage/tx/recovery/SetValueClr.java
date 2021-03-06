package org.vanilladb.core.storage.tx.recovery;

import static org.vanilladb.core.sql.Type.BIGINT;

import java.util.List;

import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.log.BasicLogRecord;
import org.vanilladb.core.storage.log.LogSeqNum;
import org.vanilladb.core.storage.tx.Transaction;

public class SetValueClr extends SetValueRecord implements CompesationLogRecord {

	private LogSeqNum undoNextLSN;

	public SetValueClr(long txNum, BlockId blk, int offset, Constant val, Constant newVal, LogSeqNum undoNextLSN) {
		super(txNum, blk, offset, val, newVal);
		this.undoNextLSN = undoNextLSN;
	}

	public SetValueClr(BasicLogRecord rec) {
		super(rec);
		undoNextLSN = new LogSeqNum((Long) rec.nextVal(BIGINT).asJavaVal(), (Long) rec.nextVal(BIGINT).asJavaVal());
	}

	@Override
	public int op() {
		return OP_SET_VALUE_CLR;
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
