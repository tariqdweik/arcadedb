/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.utility;

import com.arcadedb.log.LogManager;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;

public class FileUtils {
  public static final int    KILOBYTE = 1024;
  public static final int    MEGABYTE = 1048576;
  public static final int    GIGABYTE = 1073741824;
  public static final long   TERABYTE = 1099511627776L;
  public static final String UTF8_BOM = "\uFEFF";

  private static final boolean useOldFileAPI;

  static {
    boolean oldAPI = false;

    try {
      Class.forName("java.nio.file.FileSystemException");
    } catch (ClassNotFoundException ignore) {
      oldAPI = true;
    }

    useOldFileAPI = oldAPI;
  }

  public static String getStringContent(final Object iValue) {
    if (iValue == null)
      return null;

    final String s = iValue.toString();

    if (s == null)
      return null;

    if (s.length() > 1 && (s.charAt(0) == '\'' && s.charAt(s.length() - 1) == '\'' || s.charAt(0) == '"' && s.charAt(s.length() - 1) == '"'))
      return s.substring(1, s.length() - 1);

    if (s.length() > 1 && (s.charAt(0) == '`' && s.charAt(s.length() - 1) == '`'))
      return s.substring(1, s.length() - 1);

    return s;
  }

  public static boolean isLong(final String iText) {
    boolean isLong = true;
    final int size = iText.length();
    for (int i = 0; i < size && isLong; i++) {
      final char c = iText.charAt(i);
      isLong = isLong & ((c >= '0' && c <= '9'));
    }
    return isLong;
  }

  public static long getSizeAsNumber(final Object iSize) {
    if (iSize == null)
      throw new IllegalArgumentException("Size is null");

    if (iSize instanceof Number)
      return ((Number) iSize).longValue();

    String size = iSize.toString();

    boolean number = true;
    for (int i = size.length() - 1; i >= 0; --i) {
      final char c = size.charAt(i);
      if (!Character.isDigit(c)) {
        if (i > 0 || (c != '-' && c != '+'))
          number = false;
        break;
      }
    }

    if (number)
      return string2number(size).longValue();
    else {
      size = size.toUpperCase(Locale.ENGLISH);
      int pos = size.indexOf("KB");
      if (pos > -1)
        return (long) (string2number(size.substring(0, pos)).floatValue() * KILOBYTE);

      pos = size.indexOf("MB");
      if (pos > -1)
        return (long) (string2number(size.substring(0, pos)).floatValue() * MEGABYTE);

      pos = size.indexOf("GB");
      if (pos > -1)
        return (long) (string2number(size.substring(0, pos)).floatValue() * GIGABYTE);

      pos = size.indexOf("TB");
      if (pos > -1)
        return (long) (string2number(size.substring(0, pos)).floatValue() * TERABYTE);

      pos = size.indexOf('B');
      if (pos > -1)
        return (long) string2number(size.substring(0, pos)).floatValue();

      pos = size.indexOf('%');
      if (pos > -1)
        return (long) (-1 * string2number(size.substring(0, pos)).floatValue());

      // RE-THROW THE EXCEPTION
      throw new IllegalArgumentException("Size " + size + " has a unrecognizable format");
    }
  }

  public static Number string2number(final String iText) {
    if (iText.indexOf('.') > -1)
      return Double.parseDouble(iText);
    else
      return Long.parseLong(iText);
  }

  public static String getSizeAsString(final long iSize) {
    if (iSize > TERABYTE)
      return String.format("%2.2fTB", (float) iSize / TERABYTE);
    if (iSize > GIGABYTE)
      return String.format("%2.2fGB", (float) iSize / GIGABYTE);
    if (iSize > MEGABYTE)
      return String.format("%2.2fMB", (float) iSize / MEGABYTE);
    if (iSize > KILOBYTE)
      return String.format("%2.2fKB", (float) iSize / KILOBYTE);

    return String.valueOf(iSize) + "b";
  }

  public static String getDirectory(String iPath) {
    iPath = getPath(iPath);
    int pos = iPath.lastIndexOf("/");
    if (pos == -1)
      return "";

    return iPath.substring(0, pos);
  }

  public static void createDirectoryTree(final String iFileName) {
    final String[] fileDirectories = iFileName.split("/");
    for (int i = 0; i < fileDirectories.length - 1; ++i)
      new File(fileDirectories[i]).mkdir();
  }

  public static String getPath(final String iPath) {
    if (iPath == null)
      return null;
    return iPath.replace('\\', '/');
  }

  public static void checkValidName(final String iFileName) throws IOException {
    if (iFileName.contains("..") || iFileName.contains("/") || iFileName.contains("\\"))
      throw new IOException("Invalid file name '" + iFileName + "'");
  }

  public static void deleteRecursively(final File rootFile) {
    if (rootFile.exists()) {
      if (rootFile.isDirectory()) {
        final File[] files = rootFile.listFiles();
        if (files != null) {
          for (File f : files) {
            if (f.isFile()) {
              if (!f.delete()) {
                throw new IllegalStateException(String.format("Can not delete file %s", f));
              }
            } else
              deleteRecursively(f);
          }
        }
      }

      if (!rootFile.delete()) {
        throw new IllegalStateException(String.format("Can not delete file %s", rootFile));
      }
    }
  }

  public static void deleteFolderIfEmpty(final File dir) {
    if (dir != null && dir.listFiles() != null && dir.listFiles().length == 0) {
      deleteRecursively(dir);
    }
  }

  @SuppressWarnings("resource")
  public static final void copyFile(final File source, final File destination) throws IOException {
    FileChannel sourceChannel = new FileInputStream(source).getChannel();
    FileChannel targetChannel = new FileOutputStream(destination).getChannel();
    sourceChannel.transferTo(0, sourceChannel.size(), targetChannel);
    sourceChannel.close();
    targetChannel.close();
  }

  public static final void copyDirectory(final File source, final File destination) throws IOException {
    if (!destination.exists())
      destination.mkdirs();

    for (File f : source.listFiles()) {
      final File target = new File(destination.getAbsolutePath() + "/" + f.getName());
      if (f.isFile())
        copyFile(f, target);
      else
        copyDirectory(f, target);
    }
  }

  public static boolean renameFile(File from, File to) throws IOException {
    if (useOldFileAPI)
      return from.renameTo(to);

    final FileSystem fileSystem = FileSystems.getDefault();

    final Path fromPath = fileSystem.getPath(from.getAbsolutePath());
    final Path toPath = fileSystem.getPath(to.getAbsolutePath());
    Files.move(fromPath, toPath);

    return true;
  }

  public static String threadDump() {
    final StringBuilder dump = new StringBuilder();
    final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    final ThreadInfo[] threadInfos = threadMXBean.getThreadInfo(threadMXBean.getAllThreadIds(), 100);
    for (ThreadInfo threadInfo : threadInfos) {
      dump.append('"');
      dump.append(threadInfo.getThreadName());
      dump.append("\" ");
      final Thread.State state = threadInfo.getThreadState();
      dump.append("\n   java.lang.Thread.State: ");
      dump.append(state);
      final StackTraceElement[] stackTraceElements = threadInfo.getStackTrace();
      for (final StackTraceElement stackTraceElement : stackTraceElements) {
        dump.append("\n        at ");
        dump.append(stackTraceElement);
      }
      dump.append("\n\n");
    }
    return dump.toString();
  }

  public boolean deleteFile(final File file) {
    if (!file.exists())
      return true;

    try {
      final FileSystem fileSystem = FileSystems.getDefault();
      final Path path = fileSystem.getPath(file.getAbsolutePath());

      Files.delete(path);

      return true;
    } catch (IOException e) {
      e.printStackTrace();
    }
    return false;
  }

  public static String readStreamAsString(final InputStream iStream, final String iCharset) throws IOException {
    return readStreamAsString(iStream, iCharset, 0);
  }

  public static String readStreamAsString(final InputStream iStream, final String iCharset, final long limit) throws IOException {
    final StringBuffer fileData = new StringBuffer(1000);
    final BufferedReader reader = new BufferedReader(new InputStreamReader(iStream, iCharset));
    try {
      final char[] buf = new char[1024];
      int numRead;

      while ((numRead = reader.read(buf)) != -1) {
        String readData = String.valueOf(buf, 0, numRead);

        if (fileData.length() == 0 && readData.startsWith(UTF8_BOM))
          // SKIP UTF-8 BOM IF ANY
          readData = readData.substring(1);

        if (limit > 0 && fileData.length() + readData.length() > limit) {
          // LIMIT REACHED
          fileData.append(readData.substring(0, (int) (limit - fileData.length())));
          break;
        } else
          fileData.append(readData);

        if (limit > 0 && fileData.length() >= limit)
          // LIMIT REACHED
          break;
      }
    } finally {
      reader.close();
    }
    return fileData.toString();
  }

  public static void writeFile(final File iFile, final String iContent) throws IOException {
    final FileOutputStream fos = new FileOutputStream(iFile);
    try {
      final OutputStreamWriter os = new OutputStreamWriter(fos);
      try {
        final BufferedWriter writer = new BufferedWriter(os);
        try {
          writer.write(iContent);
        } finally {
          writer.close();
        }
      } finally {
        os.close();
      }
    } finally {
      fos.close();
    }
  }

  public static String encode(final String value, final String encoding) {
    try {
      return java.net.URLEncoder.encode(value, encoding);
    } catch (UnsupportedEncodingException e) {
      LogManager.instance().error(FileUtils.class, "Error on using encoding " + encoding, e);
      return value;
    }
  }
}
