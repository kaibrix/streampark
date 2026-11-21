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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.web.server.ResponseStatusException;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Repository
class ManagedFlinkStore {

  private final JdbcTemplate sql;

  private final ObjectMapper json =
      new ObjectMapper()
          .configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true)
          .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);

  ManagedFlinkStore(JdbcTemplate sql) {
    this.sql = sql;
  }

  JdbcTemplate jdbc() {
    return sql;
  }

  Map<String, Object> find(String query, Object... parameters) {
    List<Map<String, Object>> rows = sql.queryForList(query, parameters);
    return rows.isEmpty() ? null : rows.get(0);
  }

  Map<String, Object> require(String query, Object... parameters) {
    Map<String, Object> row = find(query, parameters);
    check(row != null, HttpStatus.NOT_FOUND, "Managed Flink object not found");
    return row;
  }

  String encode(Object value) {
    try {
      return json.writeValueAsString(value);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Invalid managed Flink document", e);
    }
  }

  <T> T decode(String value, Class<T> type) {
    try {
      return json.readValue(value, type);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Invalid managed Flink document", e);
    }
  }

  static String text(Map<String, Object> row, String field) {
    Object value = row.get(field);
    return value == null ? null : value.toString();
  }

  static long number(Map<String, Object> row, String field) {
    return ((Number) row.get(field)).longValue();
  }

  static boolean bool(Map<String, Object> row, String field) {
    Object value = row.get(field);
    return Boolean.TRUE.equals(value)
        || (value instanceof Number && ((Number) value).intValue() == 1);
  }

  static String id() {
    return UUID.randomUUID().toString();
  }

  static String hash(String content) {
    return hex(sha256().digest(content.getBytes(StandardCharsets.UTF_8)));
  }

  static MessageDigest sha256() {
    try {
      return MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }

  static String hex(byte[] bytes) {
    StringBuilder hex = new StringBuilder(bytes.length * 2);
    for (byte b : bytes) {
      hex.append(Character.forDigit((b >>> 4) & 15, 16));
      hex.append(Character.forDigit(b & 15, 16));
    }
    return hex.toString();
  }

  static String required(String value, int max) {
    check(
        value != null && !value.trim().isEmpty() && value.length() <= max,
        HttpStatus.BAD_REQUEST,
        "Missing or invalid managed Flink field");
    return value.trim();
  }

  static void check(boolean allowed, HttpStatus status, String error) {
    if (!allowed) {
      throw new ResponseStatusException(status, error);
    }
  }
}
