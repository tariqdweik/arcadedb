package com.arcadedb.server.log;

import java.util.logging.Level;

/**
 * Exposes logging to server components.
 */
public interface ServerLogger {

    default void log(final Object requester, final Level level, final String message) {
    }

    default void log(final Object requester, final Level level, final String message, final Object arg1) {
    }

    default void log(final Object requester, final Level level, final String message, final Object arg1, final Object arg2) {
    }

    default void log(final Object requester, final Level level, final String message, final Object arg1, final Object arg2, final Object arg3) {
    }

    default void log(final Object requester, final Level level, final String message, final Object arg1, final Object arg2, final Object arg3, final Object arg4) {
    }

    default void log(final Object requester, final Level level, final String message, final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5) {
    }

    default void log(final Object requester, final Level level, final String message, final Object... args) {
    }

}