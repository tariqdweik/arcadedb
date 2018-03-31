package com.arcadedb.database.async;

import com.arcadedb.database.PRID;
import com.arcadedb.utility.PLogManager;

public class PDatabaseAsyncCommand {
  private final POkCallback    onOkCallback;
  private final PErrorCallback onErrorCallback;

  public PDatabaseAsyncCommand(final POkCallback onOkCallback, final PErrorCallback onErrorCallback) {
    this.onOkCallback = onOkCallback;
    this.onErrorCallback = onErrorCallback;
  }

  public void onOk(final PRID record) {
    if (onOkCallback != null) {
      try {
        onOkCallback.call(record);
      } catch (Exception e) {
        PLogManager.instance().error(this, "Error on invoking onOk() callback for asynchronous operation %s", e, this);
      }
    }
  }

  public void onError(final PRID record, final Exception e) {
    if (onErrorCallback != null) {
      try {
        onErrorCallback.call(record, e);
      } catch (Exception e1) {
        PLogManager.instance().error(this, "Error on invoking onError() callback for asynchronous operation %s", e, this);
      }
    }
  }
}
