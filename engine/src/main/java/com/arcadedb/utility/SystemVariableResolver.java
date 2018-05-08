/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.utility;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Resolve system variables embedded in a String.
 *
 * @author Luca Garulli
 */
public class SystemVariableResolver implements VariableParserListener {
  public static final String VAR_BEGIN = "${";
  public static final String VAR_END   = "}";

  private static SystemVariableResolver instance = new SystemVariableResolver();

  public static String resolveSystemVariables(final String iPath) {
    return resolveSystemVariables(iPath, null);
  }

  public static String resolveSystemVariables(final String iPath, final String iDefault) {
    if (iPath == null)
      return iDefault;

    return (String) VariableParser.resolveVariables(iPath, VAR_BEGIN, VAR_END, instance, iDefault);
  }

  public static String resolveVariable(final String variable) {
    if (variable == null)
      return null;

    String resolved = System.getProperty(variable);

    if (resolved == null)
      // TRY TO FIND THE VARIABLE BETWEEN SYSTEM'S ENVIRONMENT PROPERTIES
      resolved = System.getenv(variable);

    return resolved;
  }

  @Override
  public String resolve(final String variable) {
    return resolveVariable(variable);
  }

  public static void setEnv(final String name, String value) {
    final Map<String, String> map = new HashMap<String, String>(System.getenv());
    map.put(name, value);
    setEnv(map);
  }

  public static void setEnv(final Map<String, String> newenv) {
    try {
      Class<?> processEnvironmentClass = Class.forName("java.lang.ProcessEnvironment");
      Field theEnvironmentField = processEnvironmentClass.getDeclaredField("theEnvironment");
      theEnvironmentField.setAccessible(true);
      Map<String, String> env = (Map<String, String>) theEnvironmentField.get(null);
      env.putAll(newenv);
      Field theCaseInsensitiveEnvironmentField = processEnvironmentClass.getDeclaredField("theCaseInsensitiveEnvironment");
      theCaseInsensitiveEnvironmentField.setAccessible(true);
      Map<String, String> cienv = (Map<String, String>) theCaseInsensitiveEnvironmentField.get(null);
      cienv.putAll(newenv);
    } catch (NoSuchFieldException ignore) {
      try {
        Class[] classes = Collections.class.getDeclaredClasses();
        Map<String, String> env = System.getenv();
        for (Class cl : classes) {
          if ("java.util.Collections$UnmodifiableMap".equals(cl.getName())) {
            Field field = cl.getDeclaredField("m");
            field.setAccessible(true);
            Object obj = field.get(env);
            Map<String, String> map = (Map<String, String>) obj;
            map.clear();
            map.putAll(newenv);
          }
        }
      } catch (Exception e2) {
        LogManager.instance().error(SystemVariableResolver.class, "", e2);
      }
    } catch (Exception e1) {
      LogManager.instance().error(SystemVariableResolver.class, "", e1);
    }
  }
}
