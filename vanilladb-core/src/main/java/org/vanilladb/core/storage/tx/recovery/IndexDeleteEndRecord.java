package org.vanilladb.core.storage.tx.recovery;

import static org.vanilladb.core.sql.Type.BIGINT;
import static org.vanilladb.core.sql.Type.INTEGER;
import static org.vanilladb.core.sql.Type.VARCHAR;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VarcharConstant;
import org.vanilladb.core.storage.file.BlockId;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.log.BasicLogRecord;
import org.vanilladb.core.storage.log.LogSeqNum;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.record.RecordId;
import org.vanilladb.core.storage.tx.Transaction;

public class IndexDeleteEndRecord extends LogicalEndRecord implements LogRecord {
	private long txNum, recordBlockNum;
	private String tblName, fldName;
	private Constant searchKey;
	private int recordSlotId;
	private LogSeqNum lsn;

	public IndexDeleteEndRecord(long txNum, String tblName, String fldName, Constant searchKey, long recordBlockNum,
			int recordSlotId, LogSeqNum logicalStartLSN) {
		this.txNum = txNum;
		this.tblName = tblName;
		this.fldName = fldName;
		this.searchKey = searchKey;
		this.recordBlockNum = recordBlockNum;
		this.recordSlotId = recordSlotId;
		super.logicalStartLSN = logicalStartLSN;
		this.lsn = null;

	}

	public IndexDeleteEndRecord(BasicLogRecord rec) {
		txNum = (Long) rec.nextVal(BIGINT).asJavaVal();
		tblName = (String) rec.nextVal(VARCHAR).asJavaVal();
		fldName = (String) rec.nextVal(VARCHAR).asJavaVal();
		int keyType = (Integer) rec.nextVal(INTEGER).asJavaVal();
		searchKey = rec.nextVal(Type.newInstance(keyType));
		recordBlockNum = (Long) rec.nextVal(BIGINT).asJavaVal();
		recordSlotId = (Integer) rec.nextVal(INTEGER).asJavaVal();
		super.logicalStartLSN = new LogSeqNum((Long) rec.nextVal(BIGINT).asJavaVal(),
				(Long) rec.nextVal(BIGINT).asJavaVal());
		lsn = rec.getLSN();
	}

	@Override
	public LogSeqNum writeToLog() {
		List<Constant> rec = buildRecord();
		return logMgr.append(rec.toArray(new Constant[rec.size()]));
	}

	@Override
	public int op() {
		return OP_INDEX_FILE_DELETE_END;
	}

	@Override
	public long txNumber() {
		return txNum;
	}

	@Override
	public void undo(Transaction tx) {

		Map<String, IndexInfo> iiMap = VanillaDb.catalogMgr().getIndexInfo(tblName, tx);
		BlockId blk = new BlockId(tblName + ".tbl", recordBlockNum);
		RecordId rid = new RecordId(blk, recordSlotId);
		IndexInfo ii = iiMap.get(fldName);
		if (ii != null) {
			Index idx = ii.open(tx);
			idx.insert(searchKey, rid, false);
			idx.close();
		}
		// Append a Logical Abort log at the end of the LogRecords
		LogSeqNum lsn = tx.recoveryMgr().logLogicalAbort(this.txNum,this.logicalStartLSN);
		VanillaDb.logMgr().flush(lsn);

	}

	/**
	 * Logical Record should not be redo since it would not do the same physical
	 * operations as the time it terminated.
	 * 
	 * @see LogRecord#redo(Transaction)
	 */
	@Override
	public void redo(Transaction tx) {
		// do nothing

	}

	@Override
	public String toString() {
		return "<INDEX DELETE END " + txNum + " " + tblName + " " + fldName + " " + searchKey.getType().getSqlType()
				+ " " + recordBlockNum + " " + recordSlotId + " " + super.logicalStartLSN + ">";
	}

	@Override
	public List<Constant> buildRecord() {
		List<Constant> rec = new LinkedList<Constant>();
		rec.add(new IntegerConstant(op()));
		rec.add(new BigIntConstant(txNum));
		rec.add(new VarcharConstant(tblName));
		rec.add(new VarcharConstant(fldName));
		rec.add(new IntegerConstant(searchKey.getType().getSqlType()));
		rec.add(searchKey);
		rec.add(new BigIntConstant(recordBlockNum));
		rec.add(new IntegerConstant(recordSlotId));
		rec.add(new BigIntConstant(super.logicalStartLSN.blkNum()));
		rec.add(new BigIntConstant(super.logicalStartLSN.offset()));
		return rec;
	}

	@Override
	public LogSeqNum getLSN() {
		return lsn;
	}

}
