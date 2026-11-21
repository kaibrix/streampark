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

import org.apache.streampark.console.core.managedflink.ManagedFlinkModels.Credentials;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;

@Component
class ManagedFlinkCredentials {

  private static final int NONCE_BYTES = 12;
  private final String root;
  private final ManagedFlinkStore store;
  private final SecureRandom random = new SecureRandom();

  ManagedFlinkCredentials(
      @Value("${streampark.managed-flink.credential-key:}") String root, ManagedFlinkStore store) {
    this.root = root;
    this.store = store;
  }

  String seal(String binding, Credentials credentials) {
    byte[] nonce = new byte[NONCE_BYTES];
    random.nextBytes(nonce);
    byte[] body =
        crypt(
            Cipher.ENCRYPT_MODE,
            binding,
            nonce,
            store.encode(credentials).getBytes(StandardCharsets.UTF_8));
    byte[] envelope = Arrays.copyOf(nonce, nonce.length + body.length);
    System.arraycopy(body, 0, envelope, nonce.length, body.length);
    return Base64.getEncoder().encodeToString(envelope);
  }

  Credentials open(String binding, String stored) {
    if (stored == null) {
      return null;
    }
    byte[] envelope = Base64.getDecoder().decode(stored);
    byte[] nonce = Arrays.copyOf(envelope, NONCE_BYTES);
    byte[] body = Arrays.copyOfRange(envelope, NONCE_BYTES, envelope.length);
    return store.decode(
        new String(crypt(Cipher.DECRYPT_MODE, binding, nonce, body), StandardCharsets.UTF_8),
        Credentials.class);
  }

  private byte[] crypt(int mode, String binding, byte[] nonce, byte[] data) {
    byte[] key = Base64.getDecoder().decode(root);
    ManagedFlinkStore.check(
        key.length == 32,
        HttpStatus.UNPROCESSABLE_ENTITY,
        "Managed Flink credential encryption is not configured");
    try {
      Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
      cipher.init(mode, new SecretKeySpec(key, "AES"), new GCMParameterSpec(128, nonce));
      cipher.updateAAD(binding.getBytes(StandardCharsets.UTF_8));
      return cipher.doFinal(data);
    } catch (GeneralSecurityException e) {
      throw new IllegalStateException("Managed Flink credential could not be processed");
    } finally {
      Arrays.fill(key, (byte) 0);
    }
  }
}
