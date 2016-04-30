package org.vanilladb.core.storage.file.io.jaydio;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.vanilladb.core.storage.file.io.IoBuffer;
import org.vanilladb.core.storage.file.io.IoChannel;

import net.smacke.jaydio.buffer.AlignedDirectByteBuffer;
import net.smacke.jaydio.channel.BufferedChannel;
import net.smacke.jaydio.channel.DirectIoByteChannel;

public class JaydioDirectIoChannel implements IoChannel {

	private BufferedChannel<AlignedDirectByteBuffer> fileChannel;
	private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

	// Optimization: store the size of each table
	private long fileSize;

	public JaydioDirectIoChannel(File file) throws IOException {
		fileChannel = DirectIoByteChannel.getChannel(file, false);
		fileSize = fileChannel.size();
	}

	@Override
	public int read(IoBuffer buffer, long position) throws IOException {
		lock.readLock().lock();
		try {
			JaydioDirectByteBuffer jaydioBuffer = (JaydioDirectByteBuffer) buffer;
			return fileChannel.read(jaydioBuffer.getAlignedDirectByteBuffer(), position);
		} finally {
			lock.readLock().unlock();
		}
	}

	@Override
	public int write(IoBuffer buffer, long position) throws IOException {
		lock.writeLock().lock();
		try {
			JaydioDirectByteBuffer jaydioBuffer = (JaydioDirectByteBuffer) buffer;
			int writeSize = fileChannel.write(jaydioBuffer.getAlignedDirectByteBuffer(), position);

			// Check if we need to update the size
			if (position + writeSize > fileSize)
				fileSize = position + writeSize;

			return writeSize;
		} finally {
			lock.writeLock().unlock();
		}
	}

	@Override
	public long append(IoBuffer buffer) throws IOException {
		lock.writeLock().lock();
		try {
			JaydioDirectByteBuffer jaydioBuffer = (JaydioDirectByteBuffer) buffer;
			int appendSize = fileChannel.write(jaydioBuffer.getAlignedDirectByteBuffer(), fileSize);
			fileSize += appendSize;
			return fileSize;
		} finally {
			lock.writeLock().unlock();
		}
	}

	@Override
	public long size() throws IOException {
		lock.readLock().lock();
		try {
			return fileSize;
		} finally {
			lock.readLock().unlock();
		}
	}

	@Override
	public void close() throws IOException {
		lock.writeLock().lock();
		try {
			fileChannel.close();
		} finally {
			lock.writeLock().unlock();
		}
	}
}
