package org.vanilladb.core.storage.record;

import static org.vanilladb.core.storage.record.FileHeaderPage.NO_SLOT_BLOCKID;
import static org.vanilladb.core.storage.record.FileHeaderPage.NO_SLOT_RID;
import static org.vanilladb.core.storage.record.FileHeaderPage.OFFSET_LDS_BLOCKID;
import static org.vanilladb.core.storage.record.FileHeaderPage.OFFSET_LDS_RID;
import static org.vanilladb.core.storage.record.FileHeaderPage.OFFSET_TS_BLOCKID;
import static org.vanilladb.core.storage.record.FileHeaderPage.OFFSET_TS_RID;

import org.vanilladb.core.sql.BigIntConstant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.storage.buffer.Buffer;
import org.vanilladb.core.storage.buffer.PageFormatter;

/**
 * An object that can format a page to look like a the header of a file.
 */
public class FileHeaderFormatter extends PageFormatter {

	@Override
	public void format(Buffer buf) {
		// initial the last free slot
		setVal(buf, OFFSET_LDS_BLOCKID, new BigIntConstant(NO_SLOT_BLOCKID));
		setVal(buf, OFFSET_LDS_RID, new IntegerConstant(NO_SLOT_RID));

		// initial the tail slot
		setVal(buf, OFFSET_TS_BLOCKID, new BigIntConstant(NO_SLOT_BLOCKID));
		setVal(buf, OFFSET_TS_RID, new IntegerConstant(NO_SLOT_RID));
	}

}
