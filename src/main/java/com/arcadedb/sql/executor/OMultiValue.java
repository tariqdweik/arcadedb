package com.arcadedb.sql.executor;

import java.util.Iterator;

public class OMultiValue {
  public static boolean isMultiValue(Object left) {
    throw new UnsupportedOperationException();
  }

  public static Iterator<Object> getMultiValueIterator(Object left) {
    throw new UnsupportedOperationException();
  }

  public static Iterable<? extends Object> getMultiValueIterable(Object target) {
    throw new UnsupportedOperationException();
  }

  public static Object[] array(Object result) {
    throw new UnsupportedOperationException();
  }

  public static void remove(Object leftVal, Object rightVal, boolean b) {

  }

  public static Object getValue(Object iResult, Integer index) {
    return null;
  }

  public static Iterable<? extends Object> getMultiValueIterable(Object iRight, boolean b) {
    return null;
  }

  public static int getSize(Object iLeft) {
    return -1;
  }

  public static Object getFirstValue(Object iLeft) {
    return null;
  }
}
