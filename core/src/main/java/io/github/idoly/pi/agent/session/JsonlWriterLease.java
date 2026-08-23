package io.github.idoly.pi.agent.session;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/** Stable lock-file lease shared by all writers of one JSONL session path. */
final class JsonlWriterLease implements AutoCloseable {
    private static final ConcurrentHashMap<Path, ReentrantLock> LOCAL_LOCKS =
            new ConcurrentHashMap<>();

    private final ReentrantLock local;
    private final FileChannel channel;
    private final FileLock fileLock;

    private JsonlWriterLease(
            ReentrantLock local,
            FileChannel channel,
            FileLock fileLock
    ) {
        this.local = local;
        this.channel = channel;
        this.fileLock = fileLock;
    }

    static JsonlWriterLease acquire(Path sessionPath) throws IOException {
        Path lockPath = Path.of(sessionPath.toString() + ".lock")
                .toAbsolutePath().normalize();
        ReentrantLock local = LOCAL_LOCKS.computeIfAbsent(
                lockPath, ignored -> new ReentrantLock()
        );
        local.lock();
        FileChannel channel = null;
        try {
            java.nio.file.Files.createDirectories(lockPath.getParent());
            channel = FileChannel.open(
                    lockPath,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE
            );
            FileLock fileLock = channel.lock();
            return new JsonlWriterLease(local, channel, fileLock);
        } catch (IOException | RuntimeException | Error failure) {
            if (channel != null) {
                try {
                    channel.close();
                } catch (IOException suppressed) {
                    failure.addSuppressed(suppressed);
                }
            }
            local.unlock();
            throw failure;
        }
    }

    @Override
    public void close() {
        try {
            fileLock.release();
        } catch (IOException ignored) {
            // A completed commit must not be reported as failed during lease release.
        }
        try {
            channel.close();
        } catch (IOException ignored) {
            // The stable lock file contains no session data.
        } finally {
            local.unlock();
        }
    }
}
