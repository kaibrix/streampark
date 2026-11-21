/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.streampark.console.core.managedflink;

import org.apache.streampark.console.core.entity.Project;
import org.apache.streampark.console.core.enums.BuildState;
import org.apache.streampark.console.core.service.ProjectService;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.Map;
import java.util.Objects;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static org.apache.streampark.console.core.managedflink.ManagedFlinkStore.check;
import static org.apache.streampark.console.core.managedflink.ManagedFlinkStore.hex;
import static org.apache.streampark.console.core.managedflink.ManagedFlinkStore.id;
import static org.apache.streampark.console.core.managedflink.ManagedFlinkStore.sha256;
import static org.apache.streampark.console.core.managedflink.ManagedFlinkStore.text;

@Service
class ManagedFlinkArtifacts {

  private final ManagedFlinkStore store;
  private final ManagedFlinkCatalog catalog;
  private final ManagedFlinkJobs jobs;
  private final String directory;
  private final long maxBytes;
  @Autowired private ProjectService projects;

  ManagedFlinkArtifacts(
      ManagedFlinkStore store,
      ManagedFlinkCatalog catalog,
      ManagedFlinkJobs jobs,
      @Value("${streampark.managed-flink.artifact-directory:}") String directory,
      @Value("${streampark.managed-flink.max-artifact-bytes:104857600}") long maxBytes) {
    this.store = store;
    this.catalog = catalog;
    this.jobs = jobs;
    this.directory = directory;
    this.maxBytes = maxBytes;
  }

  String upload(long team, long app, MultipartFile file) throws IOException {
    return capture(team, app, file.getSize(), file::getInputStream, () -> {});
  }

  String importBuild(long team, long app, long projectId, String module, String jar)
      throws IOException {
    catalog.authorize(team, "app:update", "project:view");
    jobs.job(team, app, false);
    Project project = projects.getById(projectId);
    check(
        project != null && Objects.equals(project.getTeamId(), team),
        HttpStatus.NOT_FOUND,
        "Project not found");
    check(
        Objects.equals(project.getBuildState(), BuildState.SUCCESSFUL.get()),
        HttpStatus.CONFLICT,
        "A successfully built project is required");
    check(
        module != null
            && module.matches("[a-zA-Z0-9][a-zA-Z0-9_.-]{0,254}")
            && jar != null
            && jar.matches("[a-zA-Z0-9][a-zA-Z0-9_.-]{0,250}\\.jar"),
        HttpStatus.BAD_REQUEST,
        "Expected a module and JAR filename");
    Path root = project.getDistHome().toPath().toRealPath();
    Path source = root.resolve(module).resolve(jar).toRealPath();
    check(
        source.startsWith(root) && Files.isRegularFile(source),
        HttpStatus.NOT_FOUND,
        "Project artifact not found");
    return capture(
        team,
        app,
        Files.size(source),
        () -> Files.newInputStream(source),
        () -> {
          Project latest = projects.getById(projectId);
          check(
              latest != null
                  && Objects.equals(latest.getBuildState(), BuildState.SUCCESSFUL.get())
                  && Objects.equals(latest.getLastBuild(), project.getLastBuild()),
              HttpStatus.CONFLICT,
              "Project build changed while importing its JAR");
        });
  }

  private String capture(long team, long app, long size, ArtifactSource source, Runnable verify)
      throws IOException {
    catalog.authorize(team, "app:update");
    Map<String, Object> job = jobs.job(team, app, false);
    ManagedFlinkModels.Spec spec =
        store.decode(text(job, "draft_spec"), ManagedFlinkModels.Spec.class);
    check(
        spec.getType() == ManagedFlinkModels.JobType.JAR,
        HttpStatus.CONFLICT,
        "Expected a JAR job");
    check(
        !directory.trim().isEmpty(),
        HttpStatus.UNPROCESSABLE_ENTITY,
        "Artifact staging is not configured");
    check(size > 0 && size <= maxBytes, HttpStatus.PAYLOAD_TOO_LARGE, "Invalid artifact size");
    Path root =
        Paths.get(directory)
            .toAbsolutePath()
            .normalize()
            .resolve(Long.toString(team))
            .resolve(Long.toString(app));
    Files.createDirectories(root);
    Path temp = Files.createTempFile(root, "upload-", ".jar");
    try {
      MessageDigest digest = sha256();
      long total;
      try (InputStream input = source.open();
          OutputStream output = Files.newOutputStream(temp)) {
        total = copy(input, output, digest, maxBytes);
      }
      verify.run();
      check(total == size, HttpStatus.CONFLICT, "Artifact size changed during capture");
      String mainClass = readMainClass(temp);
      String sha = hex(digest.digest());
      Path artifact = root.resolve(sha + ".jar");
      try {
        Files.createLink(artifact, temp);
      } catch (FileAlreadyExistsException e) {
        try (InputStream existing = Files.newInputStream(artifact)) {
          MessageDigest existingDigest = sha256();
          check(
              copy(existing, null, existingDigest, maxBytes) == total
                  && sha.equals(hex(existingDigest.digest())),
              HttpStatus.CONFLICT,
              "Existing immutable artifact does not match");
        }
      }
      String id = id();
      try {
        store.sql.update(
            "insert into t_flink_managed_artifact "
                + "(id,app_id,digest,size_bytes,object_ref,main_class,created_at) values (?,?,?,?,?,?,?)",
            id,
            app,
            sha,
            total,
            artifact.toString(),
            mainClass,
            System.currentTimeMillis());
      } catch (DuplicateKeyException e) {
        return text(
            store.require(
                "select id from t_flink_managed_artifact where app_id=? and digest=?", app, sha),
            "id");
      }
      return id;
    } finally {
      Files.deleteIfExists(temp);
    }
  }

  @FunctionalInterface
  private interface ArtifactSource {
    InputStream open() throws IOException;
  }

  private String readMainClass(Path file) {
    try (ZipFile jar = new ZipFile(file.toFile())) {
      ZipEntry entry = jar.getEntry("META-INF/MANIFEST.MF");
      if (entry == null) {
        return null;
      }
      try (InputStream input = jar.getInputStream(entry)) {
        ByteArrayOutputStream manifest = new ByteArrayOutputStream();
        copy(input, manifest, sha256(), 65_536);
        String mainClass =
            new Manifest(new ByteArrayInputStream(manifest.toByteArray()))
                .getMainAttributes()
                .getValue(Attributes.Name.MAIN_CLASS);
        if (mainClass != null) {
          check(mainClass.length() <= 255, HttpStatus.BAD_REQUEST, "Invalid JAR main class");
        }
        return mainClass;
      }
    } catch (IOException e) {
      throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid JAR file");
    }
  }

  private long copy(InputStream input, OutputStream output, MessageDigest digest, long maximum)
      throws IOException {
    byte[] buffer = new byte[8192];
    long total = 0;
    int count;
    while ((count = input.read(buffer)) != -1) {
      total += count;
      check(total <= maximum, HttpStatus.PAYLOAD_TOO_LARGE, "Artifact size limit exceeded");
      digest.update(buffer, 0, count);
      if (output != null) {
        output.write(buffer, 0, count);
      }
    }
    return total;
  }
}
