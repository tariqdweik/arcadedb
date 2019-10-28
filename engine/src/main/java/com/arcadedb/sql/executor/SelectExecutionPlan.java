/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.sql.executor;

import com.arcadedb.exception.CommandExecutionException;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by luigidellaquila on 06/07/16.
 */
public class SelectExecutionPlan implements InternalExecutionPlan {

  private String location;

  private CommandContext ctx;

  protected List<ExecutionStepInternal> steps = new ArrayList<>();

  ExecutionStepInternal lastStep = null;

  public SelectExecutionPlan(CommandContext ctx) {
    this.ctx = ctx;
  }

  @Override
  public void close() {
    lastStep.close();
  }

  @Override
  public ResultSet fetchNext(int n) {
    return lastStep.syncPull(ctx, n);
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    StringBuilder result = new StringBuilder();
    for (int i = 0; i < steps.size(); i++) {
      ExecutionStepInternal step = steps.get(i);
      result.append(step.prettyPrint(depth, indent));
      if (i < steps.size() - 1) {
        result.append("\n");
      }
    }
    return result.toString();
  }

  @Override
  public void reset(CommandContext ctx) {
    steps.forEach(ExecutionStepInternal::reset);
  }

  public void chain(ExecutionStepInternal nextStep) {
    if (lastStep != null) {
      lastStep.setNext(nextStep);
      nextStep.setPrevious(lastStep);
    }
    lastStep = nextStep;
    steps.add(nextStep);
  }

  @Override
  public List<ExecutionStep> getSteps() {
    //TODO do a copy of the steps
    return (List) steps;
  }

  public void setSteps(List<ExecutionStepInternal> steps) {
    this.steps = steps;
    if (steps.size() > 0) {
      lastStep = steps.get(steps.size() - 1);
    } else {
      lastStep = null;
    }
  }

  @Override
  public Result toResult() {
    ResultInternal result = new ResultInternal();
    result.setProperty("type", "QueryExecutionPlan");
    result.setProperty(JAVA_TYPE, getClass().getName());
    result.setProperty("cost", getCost());
    result.setProperty("prettyPrint", prettyPrint(0, 2));
    result.setProperty("steps", steps == null ? null : steps.stream().map(x -> x.toResult()).collect(Collectors.toList()));
    return result;
  }

  @Override
  public long getCost() {
    return 0l;
  }

  public Result serialize() {
    ResultInternal result = new ResultInternal();
    result.setProperty("type", "QueryExecutionPlan");
    result.setProperty(JAVA_TYPE, getClass().getName());
    result.setProperty("cost", getCost());
    result.setProperty("prettyPrint", prettyPrint(0, 2));
    result.setProperty("steps", steps == null ? null : steps.stream().map(x -> x.serialize()).collect(Collectors.toList()));
    return result;
  }

  public void deserialize(Result serializedExecutionPlan) {
    List<Result> serializedSteps = serializedExecutionPlan.getProperty("steps");
    for (Result serializedStep : serializedSteps) {
      try {
        String className = serializedStep.getProperty(JAVA_TYPE);
        ExecutionStepInternal step = (ExecutionStepInternal) Class.forName(className).newInstance();
        step.deserialize(serializedStep);
        chain(step);
      } catch (Exception e) {
        throw new CommandExecutionException("Cannot deserialize execution step:" + serializedStep, e);
      }
    }
  }

  @Override
  public InternalExecutionPlan copy(CommandContext ctx) {
    SelectExecutionPlan copy = new SelectExecutionPlan(ctx);

    ExecutionStep lastStep = null;
    for (ExecutionStep step : this.steps) {
      ExecutionStepInternal newStep = (ExecutionStepInternal) ((ExecutionStepInternal) step).copy(ctx);
      newStep.setPrevious((ExecutionStepInternal) lastStep);
      if (lastStep != null) {
        ((ExecutionStepInternal) lastStep).setNext(newStep);
      }
      lastStep = newStep;
      copy.getSteps().add(newStep);
    }
    copy.lastStep = copy.steps.get(copy.steps.size() - 1);
    copy.location = location;
    return copy;
  }

  @Override
  public boolean canBeCached() {
    for (ExecutionStepInternal step : steps) {
      if (!step.canBeCached()) {
        return false;
      }
    }
    return true;
  }
}

