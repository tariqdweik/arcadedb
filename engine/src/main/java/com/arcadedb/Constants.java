/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb;

import com.arcadedb.log.LogManager;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Level;

public class Constants {
    public static final String PRODUCT = "ArcadeDB";
    public static final String URL = "https://arcadedata.com";
    public static final String COPYRIGHT = "Copyrights (c) 2019 Arcade Data";

    private static final Properties properties = new Properties();

    static {
        final InputStream inputStream = Constants.class.getResourceAsStream("/com/arcadedb/arcadedb.properties");
        try {
            properties.load(inputStream);
        } catch (IOException e) {
            LogManager.instance().log(Constants.class, Level.SEVERE, "Failed to load OrientDB properties", e);
        } finally {
            if (inputStream != null) {
                try {
                    inputStream.close();
                } catch (IOException ignore) {
                    // Ignore
                }
            }
        }

    }

    /**
     * @return Major part of OrientDB version
     */
    public static int getVersionMajor() {
        final String[] versions = properties.getProperty("version").split("\\.");
        if (versions.length == 0) {
            LogManager.instance().log(Constants.class, Level.SEVERE, "Can not retrieve version information for this build", null);
            return -1;
        }

        try {
            return Integer.parseInt(versions[0]);
        } catch (NumberFormatException nfe) {
            LogManager.instance().log(Constants.class, Level.SEVERE, "Can not retrieve major version information for this build", nfe);
            return -1;
        }
    }

    /**
     * @return Minor part of OrientDB version
     */
    public static int getVersionMinor() {
        final String[] versions = properties.getProperty("version").split("\\.");
        if (versions.length < 2) {
            LogManager.instance().log(Constants.class, Level.SEVERE, "Can not retrieve minor version information for this build", null);
            return -1;
        }

        try {
            return Integer.parseInt(versions[1]);
        } catch (NumberFormatException nfe) {
            LogManager.instance().log(Constants.class, Level.SEVERE, "Can not retrieve minor version information for this build", nfe);
            return -1;
        }
    }

    /**
     * @return Hotfix part of OrientDB version
     */
    @SuppressWarnings("unused")
    public static int getVersionHotfix() {
        final String[] versions = properties.getProperty("version").split("\\.");
        if (versions.length < 3) {
            return 0;
        }

        try {
            String hotfix = versions[2];
            int snapshotIndex = hotfix.indexOf("-SNAPSHOT");

            if (snapshotIndex != -1) {
                hotfix = hotfix.substring(0, snapshotIndex);
            }

            return Integer.parseInt(hotfix);
        } catch (NumberFormatException nfe) {
            LogManager.instance().log(Constants.class, Level.SEVERE, "Can not retrieve hotfix version information for this build", nfe);
            return -1;
        }
    }

    /**
     * @return Returns only current version without build number and etc.
     */
    public static String getRawVersion() {
        return properties.getProperty("version");
    }

    /**
     * Returns the complete text of the current OrientDB version.
     */
    public static String getVersion() {
        return getRawVersion()
                + " (build " + getBuildNumber()
                + " timestamp " + getTimestamp()
                + ", branch " + properties.getProperty("branch") + ")";
    }

    /**
     * Returns true if current OrientDB version is a snapshot.
     */
    public static boolean isSnapshot() {
        return properties.getProperty("version").endsWith("SNAPSHOT");
    }

    /**
     * @return the build number if any.
     */
    public static String getBuildNumber() {
        return properties.getProperty("buildNumber");
    }

    /**
     * @return the build number if any.
     */
    public static String getTimestamp() {
        return properties.getProperty("timestamp");
    }
}