package org.vanilladb.core.storage.file.io.jaydio;

import net.smacke.jaydio.DirectIoLib;
import net.smacke.jaydio.buffer.AlignedDirectByteBuffer;

import org.vanilladb.core.storage.file.FileMgr;
import org.vanilladb.core.storage.file.io.IoBuffer;

public class JaydioDirectByteBuffer implements IoBuffer {

	private AlignedDirectByteBuffer byteBuffer;
	
	public JaydioDirectByteBuffer(int capacity) {
		byteBuffer = AlignedDirectByteBuffer
				.allocate(DirectIoLib.getLibForPath(FileMgr.DB_FILES_DIR), capacity);
	}
	
	@Override
	public IoBuffer get(int position, byte[] dst) {
		byteBuffer.position(position);
		byteBuffer.get(dst);
		return this;
	}

	@Override
	public IoBuffer put(int position, byte[] src) {
		byteBuffer.position(position);
		byteBuffer.put(src);
		return this;
	}
	
	@Override
	public void clear() {
		byteBuffer.clear();
	}

	@Override
	public void rewind() {
		byteBuffer.rewind();
	}

	@Override
	public void close() {
		byteBuffer.close();
	}
	
	AlignedDirectByteBuffer getAlignedDirectByteBuffer() {
		return byteBuffer;
	}
}
