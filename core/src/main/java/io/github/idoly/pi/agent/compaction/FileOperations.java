package io.github.idoly.pi.agent.compaction;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

public final class FileOperations {
    private final LinkedHashSet<String> read = new LinkedHashSet<>();
    private final LinkedHashSet<String> written = new LinkedHashSet<>();
    private final LinkedHashSet<String> edited = new LinkedHashSet<>();

    public Set<String> read() { return Set.copyOf(read); }

    public Set<String> written() { return Set.copyOf(written); }

    public Set<String> edited() { return Set.copyOf(edited); }

    public void addRead(String path) { if (path != null && !path.isEmpty()) read.add(path); }

    public void addWritten(String path) { if (path != null && !path.isEmpty()) written.add(path); }

    public void addEdited(String path) { if (path != null && !path.isEmpty()) edited.add(path); }

    void addReads(Collection<String> paths) { paths.forEach(this::addRead); }

    void addEdited(Collection<String> paths) { paths.forEach(this::addEdited); }

    public FileOperations copy() {
        FileOperations copy = new FileOperations();
        copy.read.addAll(read);
        copy.written.addAll(written);
        copy.edited.addAll(edited);
        return copy;
    }
}
