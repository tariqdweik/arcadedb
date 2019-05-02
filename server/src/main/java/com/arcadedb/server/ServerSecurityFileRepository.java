package com.arcadedb.server;

import com.arcadedb.utility.FileUtils;
import org.json.JSONObject;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ServerSecurityFileRepository {

    private final String securityConfPath;

    public ServerSecurityFileRepository(String securityConfPath) {
        this.securityConfPath = securityConfPath;
    }

    public boolean isSecurityConfPresent() {
        return new File(securityConfPath).exists();
    }

    public void saveConfiguration(Map<String, ServerSecurity.ServerUser> serverUsers) throws IOException {
        final File file = new File(securityConfPath);
        if (!file.exists())
            file.getParentFile().mkdirs();

        final JSONObject root = new JSONObject();

        final JSONObject users = new JSONObject();
        root.put("users", users);

        for (ServerSecurity.ServerUser u : serverUsers.values()) {
            final JSONObject user = new JSONObject();
            users.put(u.name, user);

            user.put("name", u.name);
            user.put("password", u.password);
            user.put("databaseBlackList", u.databaseBlackList);
            user.put("databases", u.databases);
        }

        final FileWriter writer =  new FileWriter(file);
        writer.write(root.toString());
        writer.close();
    }

    public Map<String, ServerSecurity.ServerUser> loadConfiguration() throws IOException {
        final File file = new File(securityConfPath);

        final JSONObject json = new JSONObject(FileUtils.readStreamAsString(new FileInputStream(file), "UTF-8"));
        final JSONObject users = json.getJSONObject("users");
        final Map<String, ServerSecurity.ServerUser> serverUsers = new HashMap<>();
        for (String user : users.keySet()) {
            final JSONObject userObject = users.getJSONObject(user);

            final List<String> databases = new ArrayList<>();

            for (Object o : userObject.getJSONArray("databases").toList())
                databases.add(o.toString());

            final ServerSecurity.ServerUser serverUser = new ServerSecurity.ServerUser(user, userObject.getString("password"), userObject.getBoolean("databaseBlackList"), databases);
            serverUsers.put(user, serverUser);
        }
        return serverUsers;
    }

}
