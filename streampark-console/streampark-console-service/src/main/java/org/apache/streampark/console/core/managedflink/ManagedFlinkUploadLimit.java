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

import io.undertow.server.HttpServerExchange;
import io.undertow.servlet.handlers.ServletRequestContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.web.filter.OncePerRequestFilter;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;

@Configuration
@ConditionalOnProperty(name = "streampark.managed-flink.enabled", havingValue = "true")
class ManagedFlinkUploadLimit {
  @Bean
  FilterRegistrationBean<OncePerRequestFilter> managedUploadLimit(
      @Value("${streampark.managed-flink.max-artifact-bytes:104857600}") long maxBytes) {
    // Allow space for multipart headers and the boundary around the JAR.
    long limit = Math.min(Math.max(0, maxBytes), Long.MAX_VALUE - 65536) + 65536;
    OncePerRequestFilter filter =
        new OncePerRequestFilter() {
          @Override
          protected void doFilterInternal(
              HttpServletRequest request, HttpServletResponse response, FilterChain chain)
              throws ServletException, IOException {
            String type = request.getContentType();
            if (type != null && type.regionMatches(true, 0, "multipart/", 0, 10)) {
              if (request.getContentLengthLong() > limit) {
                response.sendError(HttpServletResponse.SC_REQUEST_ENTITY_TOO_LARGE);
                return;
              }
              HttpServerExchange exchange = ServletRequestContext.requireCurrent().getExchange();
              long existing = exchange.getMaxEntitySize();
              exchange.setMaxEntitySize(existing > 0 ? Math.min(existing, limit) : limit);
            }
            chain.doFilter(request, response);
          }
        };
    FilterRegistrationBean<OncePerRequestFilter> registration =
        new FilterRegistrationBean<>(filter);
    registration.addUrlPatterns("/flink/managed/*");
    registration.setOrder(Ordered.HIGHEST_PRECEDENCE);
    return registration;
  }
}
