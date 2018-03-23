/* Generated By:JJTree: Do not edit this line. OMultiMatchPathItem.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.arcadedb.sql.parser;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.arcadedb.database.PIdentifiable;

import java.util.*;
import java.util.stream.Collectors;

public class OMultiMatchPathItem extends OMatchPathItem {
  protected List<OMatchPathItem> items = new ArrayList<OMatchPathItem>();

  public OMultiMatchPathItem(int id) {
    super(id);
  }

  public OMultiMatchPathItem(OrientSql p, int id) {
    super(p, id);
  }

  /**
   * Accept the visitor.
   **/
  public Object jjtAccept(OrientSqlVisitor visitor, Object data) {
    return visitor.visit(this, data);
  }

  public boolean isBidirectional() {
    return false;
  }

  public void toString(Map<Object, Object> params, StringBuilder builder) {
    builder.append(".(");
    for (OMatchPathItem item : items) {
      item.toString(params, builder);
    }
    builder.append(")");
    if (filter != null) {
      filter.toString(params, builder);
    }
  }

  protected Iterable<PIdentifiable> traversePatternEdge(OMatchStatement.MatchContext matchContext, PIdentifiable startingPoint,
      OCommandContext iCommandContext) {
    Set<PIdentifiable> result = new HashSet<PIdentifiable>();
    result.add(startingPoint);
    for (OMatchPathItem subItem : items) {
      Set<PIdentifiable> startingPoints = result;
      result = new HashSet<PIdentifiable>();
      for (PIdentifiable sp : startingPoints) {
        Iterable<PIdentifiable> subResult = subItem.executeTraversal(matchContext, iCommandContext, sp, 0);
        if (subResult instanceof Collection) {
          result.addAll((Collection) subResult);
        } else {
          for (PIdentifiable id : subResult) {
            result.add(id);
          }
        }
      }
    }
    return result;
  }

  @Override public OMultiMatchPathItem copy() {
    OMultiMatchPathItem result = (OMultiMatchPathItem) super.copy();
    result.items = items == null ? null : items.stream().map(x -> x.copy()).collect(Collectors.toList());
    return result;
  }

  @Override public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    if (!super.equals(o))
      return false;

    OMultiMatchPathItem that = (OMultiMatchPathItem) o;

    if (items != null ? !items.equals(that.items) : that.items != null)
      return false;

    return true;
  }

  @Override public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (items != null ? items.hashCode() : 0);
    return result;
  }

  public List<OMatchPathItem> getItems() {
    return items;
  }

  public void setItems(List<OMatchPathItem> items) {
    this.items = items;
  }
}
/* JavaCC - OriginalChecksum=f18f107768de80b8941f166d7fafb3c0 (do not edit this line) */
