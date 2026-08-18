/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.polaris.service.events.listeners;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

/**
 * The namespaces and tables the reverse sync is permitted to push to HMS.
 *
 * <p>Read from an object and re-read on a TTL, so changing what syncs does not require restarting
 * Polaris. The same list can be shared with the forward sync inside HMS, which reads it the same
 * way, so enabling a namespace is one upload covering both directions.
 *
 * <p>Unconfigured means every namespace in the configured catalog, which is what this listener did
 * before the allowlist existed. That is deliberately backwards compatible so deploying it cannot
 * silently stop an in-flight migration; it warns at startup instead. Once a source is configured,
 * it fails closed: a fetch that fails keeps the last list that parsed, and no error path ever
 * widens the set.
 */
final class HmsSyncAllowlist {

  private static final Logger LOG = LoggerFactory.getLogger(HmsSyncAllowlist.class);

  private final String uri;
  private final long ttlMillis;
  private final Set<String> fallback;

  private volatile Set<String> entries;
  private volatile long fetchedAt;
  private volatile boolean everLoaded;

  HmsSyncAllowlist(String uri, long ttlMillis, Set<String> fallback) {
    this.uri = uri == null ? "" : uri.trim();
    this.ttlMillis = ttlMillis;
    this.fallback = fallback;
    this.entries = fallback;
    this.everLoaded = false;
    this.fetchedAt = 0L;
  }

  /** True when no list is configured at all, in which case everything syncs. */
  boolean unrestricted() {
    return uri.isEmpty() && fallback.isEmpty();
  }

  boolean contains(String namespace, String tableName) {
    if (unrestricted()) {
      return true;
    }
    Set<String> current = current();
    if (current.isEmpty()) {
      return false;
    }
    String ns = namespace.toLowerCase(Locale.ROOT);
    return current.contains(ns) || current.contains(ns + "." + tableName.toLowerCase(Locale.ROOT));
  }

  Set<String> current() {
    if (uri.isEmpty()) {
      return fallback;
    }
    if (System.currentTimeMillis() - fetchedAt > ttlMillis) {
      refresh();
    }
    return entries;
  }

  private synchronized void refresh() {
    if (System.currentTimeMillis() - fetchedAt <= ttlMillis) {
      return; // another thread just did it
    }
    try {
      Set<String> parsed = read();
      if (!parsed.equals(entries)) {
        LOG.info("Reverse sync allowlist reloaded from {}: {}", uri, parsed);
      }
      entries = parsed;
      everLoaded = true;
    } catch (Exception e) {
      LOG.warn(
          "Could not read reverse sync allowlist from {}; keeping {} ({})",
          uri,
          everLoaded ? "the last list read" : "the configured fallback",
          e.getMessage());
    } finally {
      fetchedAt = System.currentTimeMillis();
    }
  }

  private Set<String> read() throws Exception {
    String withoutScheme = uri.substring(uri.indexOf("://") + 3);
    int slash = withoutScheme.indexOf('/');
    String bucket = withoutScheme.substring(0, slash);
    String key = withoutScheme.substring(slash + 1);

    Set<String> parsed = new HashSet<>();
    try (S3Client s3 = S3Client.create();
        BufferedReader reader =
            new BufferedReader(
                new InputStreamReader(
                    s3.getObject(GetObjectRequest.builder().bucket(bucket).key(key).build()),
                    StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        String entry = line.trim();
        int comment = entry.indexOf('#');
        if (comment >= 0) {
          entry = entry.substring(0, comment).trim();
        }
        if (!entry.isEmpty()) {
          parsed.add(entry.toLowerCase(Locale.ROOT));
        }
      }
    }
    return Collections.unmodifiableSet(parsed);
  }

  static Set<String> parseEntries(Optional<String> raw) {
    Set<String> entries = new HashSet<>();
    raw.ifPresent(
        value -> {
          for (String entry : value.split(",")) {
            String trimmed = entry.trim().toLowerCase(Locale.ROOT);
            if (!trimmed.isEmpty()) {
              entries.add(trimmed);
            }
          }
        });
    return Collections.unmodifiableSet(entries);
  }
}
