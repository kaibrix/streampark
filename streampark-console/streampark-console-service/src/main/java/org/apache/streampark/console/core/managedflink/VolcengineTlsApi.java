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

import org.apache.streampark.console.core.managedflink.VolcengineOpenApi.Reply;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.squareup.okhttp.HttpUrl;
import com.squareup.okhttp.MediaType;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.RequestBody;
import com.squareup.okhttp.Response;
import com.squareup.okhttp.ResponseBody;
import com.volcengine.sign.Credentials;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

import static org.apache.streampark.console.core.managedflink.ManagedFlinkStore.hash;
import static org.apache.streampark.console.core.managedflink.ManagedFlinkStore.hex;
import static org.apache.streampark.console.core.managedflink.VolcengineOpenApi.text;

final class VolcengineTlsApi implements VolcengineTlsLogs.SearchApi {
  private static final String JSON = "application/json";
  private static final DateTimeFormatter DATE =
      DateTimeFormatter.ofPattern("uuuuMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC);
  private static final long RESPONSE_LIMIT = 8 * 1024 * 1024;

  private final String region;
  private final Credentials credentials;

  VolcengineTlsApi(String region, Credentials credentials) {
    this.region = region;
    this.credentials = credentials;
  }

  @Override
  public Reply index(String topic) throws IOException {
    return call("GET", "DescribeIndex", topic, "");
  }

  @Override
  public Reply topic(String topic) throws IOException {
    return call("GET", "DescribeTopic", topic, "");
  }

  @Override
  public Reply search(JsonObject body) throws IOException {
    return call("POST", "SearchLogs", null, body.toString());
  }

  private Reply call(String method, String path, String topic, String payload) throws IOException {
    HttpUrl.Builder endpoint =
        new HttpUrl.Builder()
            .scheme("https")
            .host("tls-" + region + ".volces.com")
            .addPathSegment(path);
    if (topic != null) {
      endpoint.addQueryParameter("TopicId", topic);
    }
    HttpUrl url = endpoint.build();
    Request.Builder request = new Request.Builder().url(url);
    headers(method, url, region, credentials, payload, Instant.now()).forEach(request::header);
    if ("POST".equals(method)) {
      request.post(
          RequestBody.create(MediaType.parse(JSON), payload.getBytes(StandardCharsets.UTF_8)));
    }
    Response response = VolcengineOpenApi.TRANSPORT.newCall(request.build()).execute();
    try (ResponseBody body = response.body()) {
      String requestId = response.header("X-Tls-Requestid");
      if (response.code() == 401 || response.code() == 403 || response.code() == 429) {
        return new Reply(response.code(), null, requestId, null, false);
      }
      if (body == null || body.source().request(RESPONSE_LIMIT + 1)) {
        throw new IOException("Invalid TLS response size");
      }
      JsonObject json = JsonParser.parseString(body.string()).getAsJsonObject();
      String error = text(json, "ErrorCode");
      return new Reply(response.code(), json, requestId, error, error == null);
    } catch (RuntimeException e) {
      throw new IOException("Invalid TLS response");
    }
  }

  // TLS uses a path-based V4 request, unlike the root-path Flink OpenAPI signer.
  static Map<String, String> headers(
      String method,
      HttpUrl url,
      String region,
      Credentials credentials,
      String payload,
      Instant now)
      throws IOException {
    Map<String, String> headers = new TreeMap<>();
    String date = DATE.format(now);
    String bodyHash = hash(payload);
    headers.put("content-type", JSON);
    headers.put("host", url.host());
    headers.put("x-content-sha256", bodyHash);
    headers.put("x-date", date);
    headers.put("x-tls-apiversion", "0.3.0");
    if (!payload.isEmpty()) {
      headers.put("content-md5", contentMd5(payload));
    }
    if (credentials.getSessionToken() != null && !credentials.getSessionToken().isEmpty()) {
      headers.put("x-security-token", credentials.getSessionToken());
    }
    String names = String.join(";", headers.keySet());
    StringBuilder canonicalHeaders = new StringBuilder();
    headers.forEach(
        (name, value) -> canonicalHeaders.append(name).append(':').append(value).append('\n'));
    String query = url.encodedQuery() == null ? "" : url.encodedQuery();
    String canonical =
        method
            + "\n"
            + url.encodedPath()
            + "\n"
            + query
            + "\n"
            + canonicalHeaders
            + "\n"
            + names
            + "\n"
            + bodyHash;
    String scope = date.substring(0, 8) + "/" + region + "/TLS/request";
    try {
      byte[] key =
          hmac(credentials.getSecretKey().getBytes(StandardCharsets.UTF_8), date.substring(0, 8));
      key = hmac(hmac(hmac(key, region), "TLS"), "request");
      String signature =
          hex(hmac(key, "HMAC-SHA256\n" + date + "\n" + scope + "\n" + hash(canonical)));
      headers.put(
          "authorization",
          "HMAC-SHA256 Credential="
              + credentials.getAccessKey()
              + "/"
              + scope
              + ", SignedHeaders="
              + names
              + ", Signature="
              + signature);
      return headers;
    } catch (GeneralSecurityException e) {
      throw new IOException("TLS request could not be signed");
    }
  }

  private static String contentMd5(String payload) throws IOException {
    try {
      return hex(MessageDigest.getInstance("MD5").digest(payload.getBytes(StandardCharsets.UTF_8)))
          .toUpperCase(Locale.ROOT);
    } catch (GeneralSecurityException e) {
      throw new IOException("TLS request checksum unavailable");
    }
  }

  private static byte[] hmac(byte[] key, String value) throws GeneralSecurityException {
    Mac mac = Mac.getInstance("HmacSHA256");
    mac.init(new SecretKeySpec(key, "HmacSHA256"));
    return mac.doFinal(value.getBytes(StandardCharsets.UTF_8));
  }
}
