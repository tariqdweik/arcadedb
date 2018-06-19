/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.server;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.LRUCache;
import org.json.JSONObject;

import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;

public class ServerSecurity implements ServerPlugin {
  public class ServerUser {
    public String      name;
    public String      password;
    public boolean     databaseBlackList;
    public Set<String> databases = new HashSet<>();

    public ServerUser(final String name, final String password, final boolean databaseBlackList, final List<String> databases) {
      this.name = name;
      this.password = password;
      this.databaseBlackList = databaseBlackList;
      if (databases != null)
        this.databases.addAll(databases);
    }
  }

  private final        ArcadeDBServer                    server;
  private final        String                            configPath;
  private              ConcurrentMap<String, ServerUser> users      = new ConcurrentHashMap<>();
  private final        String                            algorithm;
  private final        SecretKeyFactory                  secretKeyFactory;
  private final        int                               saltIteration;
  private static final Random                            RANDOM     = new SecureRandom();
  private final static String                            FILE_NAME  = "security.json";
  public static final  int                               SALT_SIZE  = 32;
  private static       Map<String, String>               SALT_CACHE = null;

  public ServerSecurity(final ArcadeDBServer server, final String configPath) {
    this.server = server;
    this.configPath = configPath;
    this.algorithm = GlobalConfiguration.SERVER_SECURITY_ALGORITHM.getValueAsString();

    final int cacheSize = GlobalConfiguration.SERVER_SECURITY_SALT_CACHE_SIZE.getValueAsInteger();
    if (cacheSize > 0)
      SALT_CACHE = Collections.synchronizedMap(new LRUCache<>(cacheSize));

    saltIteration = GlobalConfiguration.SERVER_SECURITY_SALT_ITERATIONS.getValueAsInteger();

    try {
      secretKeyFactory = SecretKeyFactory.getInstance(algorithm);
    } catch (NoSuchAlgorithmException e) {
      server.log(this, Level.SEVERE, "Security algorithm '%s' not available (error=%s)", algorithm, e);
      throw new ServerSecurityException("Security algorithm '" + algorithm + "' not available", e);
    }
  }

  @Override
  public void configure(ArcadeDBServer arcadeDBServer, ContextConfiguration configuration) {
  }

  @Override
  public void startService() {
    try {
      createDefaultSecurity();
      final File f = new File(configPath + "/" + FILE_NAME);
      if (!f.exists()) {
        return;
      }

      loadConfiguration(f);
    } catch (IOException e) {
      throw new ServerException("Error on starting Security service", e);
    }
  }

  @Override
  public void stopService() {
    users.clear();
  }

  public ServerUser authenticate(final String userName, final String userPassword) {
    final ServerUser su = users.get(userName);
    if (su == null)
      throw new ServerSecurityException("User/Password not valid");

    if (!checkPassword(userPassword, su.password))
      throw new ServerSecurityException("User/Password not valid");

    return su;
  }

  public void createUser(final String name, final String password, final boolean databaseBlackList, final List<String> databases)
      throws IOException {
    users.put(name, new ServerUser(name, this.encode(password, generateRandomSalt()), databaseBlackList, databases));
    saveConfiguration();
  }

  public String getEncodedHash(final String password, final String salt, final int iterations) {
    // Returns only the last part of whole encoded password
    final SecretKeyFactory keyFactory;
    try {
      keyFactory = SecretKeyFactory.getInstance(algorithm);
    } catch (NoSuchAlgorithmException e) {
      throw new ServerSecurityException("Could NOT retrieve '" + algorithm + "' algorithm", e);
    }

    final KeySpec keySpec = new PBEKeySpec(password.toCharArray(), salt.getBytes(Charset.forName("UTF-8")), iterations, 256);
    final SecretKey secret;
    try {
      secret = keyFactory.generateSecret(keySpec);
    } catch (InvalidKeySpecException e) {
      throw new ServerSecurityException("Error on generating security key", e);
    }

    final byte[] rawHash = secret.getEncoded();
    final byte[] hashBase64 = Base64.getEncoder().encode(rawHash);

    return new String(hashBase64);
  }

  protected String encode(final String password, final String salt) {
    return this.encode(password, salt, saltIteration);
  }

  protected boolean checkPassword(final String password, final String hashedPassword) {
    // hashedPassword consist of: ALGORITHM, ITERATIONS_NUMBER, SALT and
    // HASH; parts are joined with dollar character ("$")
    final String[] parts = hashedPassword.split("\\$");
    if (parts.length != 4)
      // wrong hash format
      return false;

    final Integer iterations = Integer.parseInt(parts[1]);
    final String salt = parts[2];
    final String hash = encode(password, salt, iterations);

    return hash.equals(hashedPassword);
  }

  protected static String generateRandomSalt() {
    final byte[] salt = new byte[SALT_SIZE];
    RANDOM.nextBytes(salt);
    return new String(Base64.getEncoder().encode(salt));
  }

  protected String encode(final String password, final String salt, final int iterations) {
    if (!SALT_CACHE.isEmpty()) {
      final String encoded = SALT_CACHE.get(password + "$" + salt + "$" + iterations);
      if (encoded != null)
        // FOUND CACHED
        return encoded;
    }

    final String hash = getEncodedHash(password, salt, iterations);
    final String encoded = String.format("%s$%d$%s$%s", algorithm, iterations, salt, hash);

    // CACHE IT
    SALT_CACHE.put(password + "$" + salt + "$" + iterations, encoded);

    return encoded;
  }

  protected void createDefaultSecurity() throws IOException {
    createUser("root", "root", true, null);
  }

  private void saveConfiguration() throws IOException {
    final File file = new File(configPath + "/" + FILE_NAME);
    if (!file.exists())
      file.getParentFile().mkdirs();

    final FileWriter writer = new FileWriter(file);

    final JSONObject root = new JSONObject();

    final JSONObject users = new JSONObject();
    root.put("users", users);

    for (ServerUser u : this.users.values()) {
      final JSONObject user = new JSONObject();
      users.put(u.name, user);

      user.put("name", u.name);
      user.put("password", u.password);
      user.put("databaseBlackList", u.databaseBlackList);
      user.put("databases", u.databases);
    }

    writer.write(root.toString());
    writer.close();
  }

  private void loadConfiguration(final File file) throws IOException {
    final JSONObject json = new JSONObject(FileUtils.readStreamAsString(new FileInputStream(file), "UTF-8"));
    final JSONObject users = json.getJSONObject("users");
    for (String user : users.keySet()) {
      final JSONObject userObject = users.getJSONObject(user);

      final List<String> databases = new ArrayList<>();

      for (Object o : userObject.getJSONArray("databases").toList())
        databases.add(o.toString());

      this.users
          .put(user, new ServerUser(user, userObject.getString("password"), userObject.getBoolean("databaseBlackList"), databases));
    }
  }
}
