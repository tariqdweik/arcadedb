package com.arcadedb.engine;

import com.arcadedb.exception.PDatabaseOperationException;
import com.arcadedb.utility.PLogManager;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class PFileManager {
  private final String     path;
  private final PFile.MODE mode;

  private final List<PFile>                       files            = new ArrayList<PFile>();
  private final ConcurrentHashMap<String, PFile>  fileNameMap      = new ConcurrentHashMap<String, PFile>();
  private final ConcurrentHashMap<Integer, PFile> fileIdMap        = new ConcurrentHashMap<Integer, PFile>();
  private final Set<String>                       supportedFileExt = new HashSet<String>();
  private final AtomicLong                        maxFilesOpened   = new AtomicLong();

  public class PFileManagerStats {
    public long maxOpenFiles;
    public long totalOpenFiles;
  }

  public PFileManager(final String path, final PFile.MODE mode, final Set<String> supportedFileExt) {
    this.path = path;
    this.mode = mode;

    if (supportedFileExt != null && !supportedFileExt.isEmpty())
      this.supportedFileExt.addAll(supportedFileExt);

    File dbDirectory = new File(path);
    if (!dbDirectory.exists()) {
      dbDirectory.mkdirs();
    } else {
      for (File f : dbDirectory.listFiles()) {
        final String filePath = f.getAbsolutePath();
        final String fileExt = filePath.substring(filePath.lastIndexOf(".") + 1);

        if (supportedFileExt.contains(fileExt))
          try {
            final PFile file = new PFile(f.getAbsolutePath(), mode);
            registerFile(file);

          } catch (FileNotFoundException e) {
            PLogManager.instance().warn(this, "Cannot load file '%s'", f);
          }
      }
    }
  }

  public void close() {
    for (PFile f : fileNameMap.values())
      try {
        f.close();
      } catch (IOException e) {
        throw new PDatabaseOperationException("Error on closing file '" + f.getFileName() + "'");
      }

    files.clear();
    fileNameMap.clear();
    fileIdMap.clear();
  }

  public void dropFile(final int fileId) throws IOException {
    PFile file = fileIdMap.remove(fileId);
    if (file != null) {
      fileNameMap.remove(file.getFileName());
      files.set(fileId, null);
    }
    file.drop();
  }

  public PFileManagerStats getStats() {
    final PFileManagerStats stats = new PFileManagerStats();
    stats.maxOpenFiles = maxFilesOpened.get();
    stats.totalOpenFiles = fileIdMap.size();
    return stats;
  }

  public Collection<PFile> getFiles() {
    return fileNameMap.values();
  }

  public PFile getFile(final int fileId) {
    PFile f = fileIdMap.get(fileId);
    if (f == null)
      throw new IllegalArgumentException("File with id " + fileId + " was not found");

    return f;
  }

  public PFile getFile(final String fileName) throws FileNotFoundException {
    PFile file = fileNameMap.get(fileName);
    if (file == null) {
      synchronized (this) {
        file = new PFile(fileName, mode);
        final PFile prev = fileNameMap.putIfAbsent(fileName, file);
        if (prev == null) {
          file.setFileId(newFileId());
          registerFile(file);
        } else
          file = prev;
      }
    }
    return file;
  }

  public PFile getOrCreateFile(final String filePath, final PFile.MODE mode) throws FileNotFoundException {
    return getOrCreateFile(PFile.getFileNameFromPath(filePath), filePath, mode);
  }

  public int newFileId() {
    // LOOK FOR AN HOLE
    for (int i = 0; i < files.size(); ++i) {
      if (files.get(i) == null)
        return i;
    }
    return files.size();
  }

  public PFile getOrCreateFile(final String fileName, final String filePath, final PFile.MODE mode) throws FileNotFoundException {
    PFile file = fileNameMap.get(fileName);
    if (file != null)
      return file;

    file = new PFile(filePath, mode);
    registerFile(file);
    return file;
  }

  private void registerFile(final PFile file) {
    while (files.size() < file.getFileId() + 1)
      files.add(null);
    files.set(file.getFileId(), file);
    fileNameMap.put(file.getFileName(), file);
    fileIdMap.put(file.getFileId(), file);
    maxFilesOpened.incrementAndGet();
  }

}
