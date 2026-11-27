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

import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Context;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.DependencyArtifact;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.FrozenCommand;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Observation;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.OperationStep;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Outcome;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.volcengine.sign.Credentials;
import com.volcengine.tos.TOSV2;
import com.volcengine.tos.TOSV2ClientBuilder;
import com.volcengine.tos.TosException;
import com.volcengine.tos.TosServerException;
import com.volcengine.tos.auth.StaticCredentials;
import com.volcengine.tos.model.object.GetObjectV2Input;
import com.volcengine.tos.model.object.GetObjectV2Output;
import com.volcengine.tos.model.object.PutObjectFromFileInput;
import com.volcengine.tos.transport.TransportConfig;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.function.BooleanSupplier;

import static org.apache.streampark.console.core.managedflink.ManagedFlinkStore.hex;
import static org.apache.streampark.console.core.managedflink.ManagedFlinkStore.sha256;

final class VolcengineArtifactStore {
  String importObject(
      Context context,
      Credentials credentials,
      String uri,
      ManagedFlinkProvider.ArtifactCapture capture)
      throws IOException {
    String bucket = context.getTarget().getArtifactBucket();
    String prefix = context.getTarget().getArtifactPrefix();
    String bucketUri = "tos://" + bucket + "/";
    String pathPrefix = bucketUri + prefix + "/";
    if (uri == null
        || uri.length() > bucketUri.length() + 1024
        || !uri.startsWith(pathPrefix)
        || !uri.substring(pathPrefix.length()).matches("([\\w.-]+/)*[\\w.-]+\\.jar")
        || java.util.Arrays.stream(uri.substring(pathPrefix.length()).split("/"))
            .anyMatch(part -> part.equals(".") || part.equals(".."))) {
      throw new ResponseStatusException(
          HttpStatus.BAD_REQUEST, "Expected a TOS JAR in the environment artifact prefix");
    }
    try (TOSV2 tos = client(context, credentials);
        GetObjectV2Output object =
            tos.getObject(
                new GetObjectV2Input()
                    .setBucket(bucket)
                    .setKey(uri.substring(bucketUri.length())))) {
      return capture.capture(object.getContentLength(), object.getContent());
    } catch (TosException e) {
      throw new ResponseStatusException(
          HttpStatus.UNPROCESSABLE_ENTITY, "Cloud artifact is unreadable");
    }
  }

  Observation stage(
      Context context, OperationStep step, Credentials credentials, BooleanSupplier permit) {
    return stage(context, step, step.getDependency(), credentials, permit);
  }

  Observation stage(
      Context context,
      OperationStep step,
      DependencyArtifact dependency,
      Credentials credentials,
      BooleanSupplier permit) {
    FrozenCommand command = step.getCommand();
    String bucket = context.getTarget().getArtifactBucket();
    String digest = dependency == null ? command.getArtifactDigest() : dependency.getDigest();
    Long size = dependency == null ? command.getArtifactSize() : Long.valueOf(dependency.getSize());
    if (digest == null
        || !digest.matches("[a-f0-9]{64}")
        || size == null
        || size < 1
        || step.getAppId() == null) {
      return Observation.outcome(Outcome.REJECTED, "ARTIFACT_NOT_VERIFIED");
    }
    String key =
        context.getTarget().getArtifactPrefix()
            + "/"
            + context.getTeamId()
            + "/"
            + step.getAppId()
            + "/"
            + digest
            + ".jar";
    boolean uploading = false;
    try (TOSV2 tos = client(context, credentials)) {
      try (GetObjectV2Output read =
          tos.getObject(new GetObjectV2Input().setBucket(bucket).setKey(key))) {
        if (!matches(read, size, digest)) {
          return Observation.outcome(Outcome.REJECTED, "ARTIFACT_CONTENT_MISMATCH");
        }
        Observation found = Observation.outcome(Outcome.CONFIRMED, null);
        found.setArtifactRef("tos://" + bucket + "/" + key);
        if (permit != null) {
          if (!permit.getAsBoolean()) {
            return Observation.outcome(Outcome.REJECTED, "DISPATCH_NOT_AUTHORIZED");
          }
          found.setRequestId(read.getRequestInfo().getRequestId());
        }
        return found;
      } catch (TosServerException e) {
        if (e.getStatusCode() != 404) {
          throw e;
        }
        if (permit == null) {
          return Observation.outcome(Outcome.WAITING, "ARTIFACT_NOT_OBSERVED");
        }
      }
      PutObjectFromFileInput input =
          new PutObjectFromFileInput()
              .setBucket(bucket)
              .setKey(key)
              .setForbidOverwrite(true)
              .setFile(
                  Paths.get(
                          dependency == null ? command.getArtifactRef() : dependency.getLocalRef())
                      .toFile());
      if (!permit.getAsBoolean()) {
        return Observation.outcome(Outcome.REJECTED, "DISPATCH_NOT_AUTHORIZED");
      }
      uploading = true;
      Observation uploaded = Observation.outcome(Outcome.WAITING, null);
      uploaded.setRequestId(tos.putObjectFromFile(input).getRequestInfo().getRequestId());
      return uploaded;
    } catch (TosServerException e) {
      Observation unavailable =
          Observation.outcome(
              uploading || permit == null ? Outcome.UNKNOWN : Outcome.REJECTED,
              "ARTIFACT_TOS_UNAVAILABLE");
      unavailable.setRequestId(e.getRequestID());
      return unavailable;
    } catch (IOException | RuntimeException e) {
      return Observation.outcome(
          uploading || permit == null ? Outcome.UNKNOWN : Outcome.REJECTED,
          "ARTIFACT_TOS_UNAVAILABLE");
    }
  }

  private TOSV2 client(Context context, Credentials profile) {
    org.slf4j.Logger sdkLogger = LoggerFactory.getLogger("com.volcengine.tos");
    if (sdkLogger instanceof Logger) {
      // The SDK formatter includes signed request fields at DEBUG level.
      ((Logger) sdkLogger).setLevel(Level.OFF);
    }
    StaticCredentials auth =
        new StaticCredentials(profile.getAccessKey(), profile.getSecretKey())
            .withSecurityToken(profile.getSessionToken());
    TransportConfig transport =
        new TransportConfig()
            .setMaxRetryCount(0)
            .setFollowRedirectTimes(0)
            .setConnectTimeoutMills(4000)
            .setReadTimeoutMills(8000)
            .setWriteTimeoutMills(8000);
    return new TOSV2ClientBuilder()
        .build(context.getRegion(), "tos-" + context.getRegion() + ".volces.com", auth, transport);
  }

  private boolean matches(GetObjectV2Output read, long size, String expected) throws IOException {
    if (read.getContentLength() != size) {
      return false;
    }
    MessageDigest digest = sha256();
    byte[] buffer = new byte[64 * 1024];
    long count = 0;
    InputStream content = read.getContent();
    for (int bytes; (bytes = content.read(buffer)) != -1; ) {
      count += bytes;
      if (count > size) {
        return false;
      }
      digest.update(buffer, 0, bytes);
    }
    return count == size && hex(digest.digest()).equals(expected);
  }
}
