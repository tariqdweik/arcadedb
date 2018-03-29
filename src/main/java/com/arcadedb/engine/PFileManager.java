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
  private final String              path;
  private final PPaginatedFile.MODE mode;

  private final List<PPaginatedFile>                       files            = new ArrayList<>();
  private final ConcurrentHashMap<String, PPaginatedFile>  fileNameMap      = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<Integer, PPaginatedFile> fileIdMap        = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<Integer, Long>           fileVirtualSize  = new ConcurrentHashMap<>();
  private final Set<String>                                supportedFileExt = new HashSet<>();
  private final AtomicLong                                 maxFilesOpened   = new AtomicLong();

  public class PFileManagerStats {
    public long maxOpenFiles;
    public long totalOpenFiles;
  }

  public PFileManager(final String path, final PPaginatedFile.MODE mode, final Set<String> supportedFileExt) throws IOException {
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
            final PPaginatedFile file = new PPaginatedFile(f.getAbsolutePath(), mode);
            registerFile(file);

          } catch (FileNotFoundException e) {
            PLogManager.instance().warn(this, "Cannot load file '%s'", f);
          }
      }
    }
  }

  public void close() {
    for (PPaginatedFile f : fileNameMap.values())
      try {
        f.close();
      } catch (IOException e) {
        throw new PDatabaseOperationException("Error on closing file '" + f.getFileName() + "'");
      }

    files.clear();
    fileNameMap.clear();
    fileIdMap.clear();
    fileVirtualSize.clear();
  }

  public void dropFile(final int fileId) throws IOException {
    PPaginatedFile file = fileIdMap.remove(fileId);
    if (file != null) {
      fileNameMap.remove(file.getFileName());
      files.set(fileId, null);
    }
    file.drop();
  }

  public long getVirtualFileSize(final Integer fileId) throws IOException {
    Long fileSize = fileVirtualSize.get(fileId);
    if (fileSize == null)
      fileSize = getFile(fileId).getSize();
    return fileSize;
  }

  public void setVirtualFileSize(final Integer fileId, final long fileSize) {
    fileVirtualSize.put(fileId, fileSize);
  }

  public PFileManagerStats getStats() {
    final PFileManagerStats stats = new PFileManagerStats();
    stats.maxOpenFiles = maxFilesOpened.get();
    stats.totalOpenFiles = fileIdMap.size();
    return stats;
  }

  public Collection<PPaginatedFile> getFiles() {
    return fileNameMap.values();
  }

  public PPaginatedFile getFile(final int fileId) {
    PPaginatedFile f = fileIdMap.get(fileId);
    if (f == null)
      throw new IllegalArgumentException("File with id " + fileId + " was not found");

    return f;
  }

  public PPaginatedFile getFile(final String fileName) throws IOException {
    PPaginatedFile file = fileNameMap.get(fileName);
    if (file == null) {
      synchronized (this) {
        file = new PPaginatedFile(fileName, mode);
        final PPaginatedFile prev = fileNameMap.putIfAbsent(fileName, file);
        if (prev == null) {
          file.setFileId(newFileId());
          registerFile(file);
        } else
          file = prev;
      }
    }
    return file;
  }

  public PPaginatedFile getOrCreateFile(final String filePath, final PPaginatedFile.MODE mode) throws IOException {
    return getOrCreateFile(PPaginatedFile.getFileNameFromPath(filePath), filePath, mode);
  }

  public int newFileId() {
    // LOOK FOR AN HOLE
    for (int i = 0; i < files.size(); ++i) {
      if (files.get(i) == null)
        return i;
    }
    return files.size();
  }

  public PPaginatedFile getOrCreateFile(final String fileName, final String filePath, final PPaginatedFile.MODE mode) throws IOException {
    PPaginatedFile file = fileNameMap.get(fileName);
    if (file != null)
      return file;

    file = new PPaginatedFile(filePath, mode);
    registerFile(file);
    return file;
  }

  private void registerFile(final PPaginatedFile file) {
    while (files.size() < file.getFileId() + 1)
      files.add(null);
    files.set(file.getFileId(), file);
    fileNameMap.put(file.getFileName(), file);
    fileIdMap.put(file.getFileId(), file);
    maxFilesOpened.incrementAndGet();
  }

}
