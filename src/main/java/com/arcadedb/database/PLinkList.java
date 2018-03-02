package com.arcadedb.database;

import java.util.ArrayList;
import java.util.List;

public class PLinkList {
  private List<PRID> list = new ArrayList<PRID>();

  public void add(final PRID rid) {
    list.add(rid);
  }

  public boolean contains(final PRID rid) {
    return list.indexOf(rid) > -1;
  }

  @Override
  public String toString() {
    return list.toString();
  }
}
