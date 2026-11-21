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

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.squareup.okhttp.Authenticator;
import com.squareup.okhttp.HttpUrl;
import com.squareup.okhttp.MediaType;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.RequestBody;
import com.squareup.okhttp.Response;
import com.squareup.okhttp.ResponseBody;
import com.volcengine.Pair;
import com.volcengine.sign.Credentials;
import com.volcengine.sign.VolcstackSign;

import java.io.IOException;
import java.net.Proxy;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

final class VolcengineOpenApi {
  static final String FLINK_VERSION = "2025-01-01";
  static final String GWS_VERSION = "2021-06-01";
  private static final HttpUrl ENDPOINT = HttpUrl.parse("https://open.volcengineapi.com/");
  private static final HttpUrl STS = HttpUrl.parse("https://sts.volcengineapi.com/");
  private static final MediaType JSON = MediaType.parse("application/json; charset=UTF-8");
  private static final long MAX_RESPONSE_BYTES = 8 * 1024 * 1024;

  private static final OkHttpClient TRANSPORT = transport();
  private final String region;
  private final Credentials credentials;

  VolcengineOpenApi(String region, Credentials credentials) {
    this.region = region;
    this.credentials = credentials;
  }

  private static OkHttpClient transport() {
    com.volcengine.observability.debugger.SdkDebugLog.SDK_CORE_LOGGER.setLogLevel(0L);
    OkHttpClient client = new OkHttpClient();
    client.setConnectTimeout(4, TimeUnit.SECONDS);
    client.setReadTimeout(8, TimeUnit.SECONDS);
    client.setWriteTimeout(8, TimeUnit.SECONDS);
    client.setRetryOnConnectionFailure(false);
    client.setFollowRedirects(false);
    client.setFollowSslRedirects(false);
    client.setAuthenticator(
        new Authenticator() {
          @Override
          public Request authenticate(Proxy proxy, Response response) {
            return null;
          }

          @Override
          public Request authenticateProxy(Proxy proxy, Response response) {
            return null;
          }
        });
    return client;
  }

  Reply call(String action, String version, Map<String, String> scope, JsonObject body)
      throws IOException {
    return call(ENDPOINT, "flink", "POST", action, version, scope, body.toString());
  }

  Reply identity() throws IOException {
    return call(
        STS, "sts", "GET", "GetCallerIdentity", "2018-01-01", java.util.Collections.emptyMap(), "");
  }

  Reply metric(JsonObject body) throws IOException {
    return call(
        ENDPOINT,
        "cloudmonitor",
        "POST",
        "GetMetricData",
        "2018-01-01",
        java.util.Collections.emptyMap(),
        body.toString());
  }

  private Reply call(
      HttpUrl endpoint,
      String service,
      String method,
      String action,
      String version,
      Map<String, String> scope,
      String payload)
      throws IOException {
    VolcstackSign signer = new VolcstackSign(credentials);
    signer.setMethod(method);
    signer.setService(service);
    signer.setRegion(region);
    List<Pair> parameters = new ArrayList<>();
    parameters.add(new Pair("Action", action));
    parameters.add(new Pair("Version", version));
    scope.forEach((name, value) -> parameters.add(new Pair(name, value)));
    Map<String, String> headers = new LinkedHashMap<>();
    headers.put("Host", endpoint.host());
    signer.applyToParams(parameters, headers, payload);
    if (!headers.containsKey("Authorization")) {
      throw new IOException("Volcengine request could not be signed");
    }
    HttpUrl.Builder url = endpoint.newBuilder();
    for (Pair pair : parameters) {
      url.addQueryParameter(pair.getName(), pair.getValue());
    }
    Request.Builder request = new Request.Builder().url(url.build());
    headers.forEach(request::header);
    if ("POST".equals(method)) {
      request.post(RequestBody.create(JSON, payload));
    }
    Response response = TRANSPORT.newCall(request.build()).execute();
    try (ResponseBody entity = response.body()) {
      if (entity == null
          || entity.contentLength() > MAX_RESPONSE_BYTES
          || entity.source().request(MAX_RESPONSE_BYTES + 1)) {
        throw new IOException("Invalid Volcengine response size");
      }
      JsonObject root = JsonParser.parseString(entity.string()).getAsJsonObject();
      JsonObject meta = object(root, "ResponseMetadata");
      JsonObject error = object(meta, "Error");
      String requestId = text(meta, "RequestId");
      return new Reply(
          response.code(),
          root.get("Result"),
          requestId,
          text(error, "Code"),
          requestId != null && !requestId.isEmpty() && !meta.has("Error"));
    } catch (RuntimeException e) {
      throw new IOException("Invalid Volcengine response envelope");
    }
  }

  static JsonObject object(JsonObject value, String field) {
    JsonElement element = value.get(field);
    return element != null && element.isJsonObject() ? element.getAsJsonObject() : new JsonObject();
  }

  static String text(JsonObject object, String field) {
    JsonElement element = object.get(field);
    return element != null && element.isJsonPrimitive() ? element.getAsString() : null;
  }

  static final class Reply {
    final int httpStatus;
    final JsonElement result;
    final String requestId;
    final String errorCode;
    final boolean businessSuccess;

    Reply(
        int status,
        JsonElement result,
        String requestId,
        String errorCode,
        boolean businessSuccess) {
      this.httpStatus = status;
      this.result = result;
      this.requestId = requestId;
      this.errorCode = errorCode;
      this.businessSuccess = businessSuccess;
    }

    boolean ok() {
      return httpStatus >= 200 && httpStatus < 300 && businessSuccess;
    }

    JsonObject data() {
      return result != null && result.isJsonObject() ? result.getAsJsonObject() : new JsonObject();
    }
  }
}
