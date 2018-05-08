package com.arcadedb.sql.executor;

import java.text.DecimalFormat;
import java.util.Optional;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public abstract class AbstractExecutionStep implements ExecutionStepInternal {

  protected final CommandContext                  ctx;
  protected       Optional<ExecutionStepInternal> prev     = Optional.empty();
  protected       Optional<ExecutionStepInternal> next     = Optional.empty();
  protected       boolean                         timedOut = false;

  protected boolean profilingEnabled = false;

  public AbstractExecutionStep(CommandContext ctx, boolean profilingEnabled) {
    this.ctx = ctx;
    this.profilingEnabled = profilingEnabled;
  }

  @Override
  public void setPrevious(ExecutionStepInternal step) {
    this.prev = Optional.ofNullable(step);
  }

  @Override
  public void setNext(ExecutionStepInternal step) {
    this.next = Optional.ofNullable(step);
  }

  public CommandContext getContext() {
    return ctx;
  }

  public Optional<ExecutionStepInternal> getPrev() {
    return prev;
  }

  public Optional<ExecutionStepInternal> getNext() {
    return next;
  }

  @Override
  public void sendTimeout() {
    this.timedOut = true;
    prev.ifPresent(p -> p.sendTimeout());
  }

  @Override
  public void close() {
    prev.ifPresent(p -> p.close());
  }

  public boolean isProfilingEnabled() {
    return profilingEnabled;
  }

  public void setProfilingEnabled(boolean profilingEnabled) {
    this.profilingEnabled = profilingEnabled;
  }

  protected String getCostFormatted() {
    return new DecimalFormat().format(getCost() / 1000) + "μs";
  }

}
