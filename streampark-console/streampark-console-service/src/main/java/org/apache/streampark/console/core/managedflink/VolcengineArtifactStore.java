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
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.FrozenCommand;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Observation;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.OperationStep;
import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Outcome;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.volcengine.ApiException;
import com.volcengine.sign.Credentials;
import com.volcengine.tos.TOSV2;
import com.volcengine.tos.TOSV2ClientBuilder;
import com.volcengine.tos.TosServerException;
import com.volcengine.tos.auth.StaticCredentials;
import com.volcengine.tos.model.object.GetObjectV2Input;
import com.volcengine.tos.model.object.GetObjectV2Output;
import com.volcengine.tos.model.object.PutObjectFromFileInput;
import com.volcengine.tos.transport.TransportConfig;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.security.MessageDigest;

import static org.apache.streampark.console.core.managedflink.ManagedFlinkStore.hex;
import static org.apache.streampark.console.core.managedflink.ManagedFlinkStore.sha256;

final class VolcengineArtifactStore {
  private final VolcengineProfileBinding binding;

  VolcengineArtifactStore(VolcengineProfileBinding binding) {
    this.binding = binding;
  }

  Observation stage(Context context, OperationStep step, boolean dispatch) {
    FrozenCommand command = step.getCommand();
    String bucket = context.getTarget().getArtifactBucket();
    String digest = command.getArtifactDigest();
    if (digest == null
        || !digest.matches("[a-f0-9]{64}")
        || command.getArtifactSize() == null
        || command.getArtifactSize() < 1
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
    try (TOSV2 tos = client(context)) {
      try (GetObjectV2Output read =
          tos.getObject(new GetObjectV2Input().setBucket(bucket).setKey(key))) {
        if (!matches(read, command.getArtifactSize(), digest)) {
          return Observation.outcome(Outcome.REJECTED, "ARTIFACT_CONTENT_MISMATCH");
        }
        Observation found = Observation.outcome(Outcome.CONFIRMED, null);
        found.setArtifactRef("tos://" + bucket + "/" + key);
        if (dispatch) {
          found.setRequestId(read.getRequestInfo().getRequestId());
        }
        return found;
      } catch (TosServerException e) {
        if (e.getStatusCode() != 404) {
          throw e;
        }
        if (!dispatch) {
          return Observation.outcome(Outcome.WAITING, "ARTIFACT_NOT_OBSERVED");
        }
      }
      Observation uploaded = Observation.outcome(Outcome.WAITING, null);
      uploaded.setRequestId(
          tos.putObjectFromFile(
                  new PutObjectFromFileInput()
                      .setBucket(bucket)
                      .setKey(key)
                      .setForbidOverwrite(true)
                      .setFile(Path.of(command.getArtifactRef()).toFile()))
              .getRequestInfo()
              .getRequestId());
      return uploaded;
    } catch (TosServerException e) {
      Observation unavailable = Observation.outcome(Outcome.UNKNOWN, "ARTIFACT_TOS_UNAVAILABLE");
      unavailable.setRequestId(e.getRequestID());
      return unavailable;
    } catch (ApiException | IOException | RuntimeException e) {
      return Observation.outcome(Outcome.UNKNOWN, "ARTIFACT_TOS_UNAVAILABLE");
    }
  }

  private TOSV2 client(Context context) throws ApiException {
    org.slf4j.Logger sdkLogger = LoggerFactory.getLogger("com.volcengine.tos");
    if (sdkLogger instanceof Logger) {
      // The SDK formatter includes signed request fields at DEBUG level.
      ((Logger) sdkLogger).setLevel(Level.OFF);
    }
    Credentials profile = binding.credentials(context);
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
