/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.server;

import com.arcadedb.ContextConfiguration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ServerSecurityTest {

    @Test
    void shouldCreateDefaultRootUserAndPersistsSecurityConfiguration() throws IOException {
        //cleanup
        final Path securityConfPath = Paths.get("./target", ServerSecurity.FILE_NAME);
        Files.deleteIfExists(securityConfPath);

        final ServerSecurity security = new ServerSecurity(new ContextConfiguration(), "./target");
        security.startService();

        File securityConf = securityConfPath.toFile();

        Assertions.assertTrue(securityConf.exists());

        ServerSecurityFileRepository repository = new ServerSecurityFileRepository(securityConfPath.toString());

        final Map<String, ServerSecurity.ServerUser> users = repository.loadConfiguration();

        Assertions.assertTrue(users.containsKey("root"));
    }

    @Test
    void shouldLoadProvidedSecurityConfiguration() throws IOException {
        //cleanup
        final Path securityConfPath = Paths.get("./target", ServerSecurity.FILE_NAME);
        Files.deleteIfExists(securityConfPath);

        //given
        ServerSecurityFileRepository repository = new ServerSecurityFileRepository(securityConfPath.toString());

        final Map<String, ServerSecurity.ServerUser> users = new HashMap<>();

        users.put("providedUser", new ServerSecurity.ServerUser("providedUser", "password", false, Collections.singletonList("database")));

        repository.saveConfiguration(users);


        //when
        final ServerSecurity security = new ServerSecurity(new ContextConfiguration(), "./target");
        security.startService();


        Assertions.assertTrue(security.existsUser("providedUser"));
        Assertions.assertFalse(security.existsUser("root" ));


    }

    @Test
    public void checkQuery() {
        final ServerSecurity security = new ServerSecurity(new ContextConfiguration(), "./target");
        security.startService();


        Assertions
                .assertEquals("PBKDF2WithHmacSHA256$65536$ThisIsTheSalt$wIKUzWYH72cKJRnFZ0PTSevERtwZTNdN+W4/Fd7xBvw=", security.encode("ThisIsATest", "ThisIsTheSalt"));
        Assertions
                .assertEquals("PBKDF2WithHmacSHA256$65536$ThisIsTheSalt$wIKUzWYH72cKJRnFZ0PTSevERtwZTNdN+W4/Fd7xBvw=", security.encode("ThisIsATest", "ThisIsTheSalt"));

        for (int i = 0; i < 1000000; ++i) {
            Assertions.assertFalse(security.generateRandomSalt().contains("$"));
        }

        security.stopService();
    }

    private void passwordShouldMatch(final ServerSecurity security, String password, String expectedHash) {
        Assertions.assertTrue(security.checkPassword(password, expectedHash));
    }

    private void passwordShouldNotMatch(final ServerSecurity security, String password, String expectedHash) {
        Assertions.assertFalse(security.checkPassword(password, expectedHash));
    }
}