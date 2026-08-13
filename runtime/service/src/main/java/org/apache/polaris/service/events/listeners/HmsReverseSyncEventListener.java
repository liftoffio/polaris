/*
 * HMS Reverse Sync Listener — Polaris → HMS
 *
 * Mirrors Iceberg table changes made via the Polaris REST catalog API
 * back to a Hive Metastore via Thrift, so that HMS-based readers see
 * the updated metadata_location.
 *
 * Configure in application.properties:
 *   polaris.event-listener.types=hms-reverse-sync
 *   polaris.hms-reverse-sync.enabled=true
 *   polaris.hms-reverse-sync.hms-host=localhost
 *   polaris.hms-reverse-sync.hms-port=9083
 *   polaris.hms-reverse-sync.catalog=liftoff_internal
 *   polaris.hms-reverse-sync.ignore-principal=hms-sync
 *   polaris.hms-reverse-sync.hms-user=polaris-sync
 */
package org.apache.polaris.service.events.listeners;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.quarkus.runtime.Startup;
import io.smallrye.common.annotation.Identifier;
import io.vertx.core.Vertx;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.service.events.EventAttributes;
import org.apache.polaris.service.events.PolarisEvent;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CDI event listener that mirrors table create/update/drop events from the
 * Polaris INTERNAL catalog back to a Hive Metastore (HMS) via Thrift, so that
 * HMS-based readers see the updated metadata_location.
 *
 * <p>Echo detection runs in both directions. Inbound: events originating from the
 * configured {@code ignore-principal} (default: {@code hms-sync}) are skipped —
 * those come from the HMS→Polaris forward sync listener and would otherwise cause
 * an infinite loop. Outbound: writes to HMS declare themselves via {@code set_ugi}
 * as {@code hms-user} (default: {@code polaris-sync}) so the forward sync listener
 * can recognize and skip them.
 *
 * <p>Metrics (Micrometer Prometheus):
 * <ul>
 *   <li>{@code polaris_hms_sync_events_total{operation}} — events dispatched to HMS worker
 *   <li>{@code polaris_hms_sync_skipped_total{reason}} — events skipped before HMS call
 *   <li>{@code polaris_hms_sync_successes_total{operation}} — HMS calls that completed without error
 *   <li>{@code polaris_hms_sync_errors_total{operation}} — HMS calls that threw an exception
 *   <li>{@code polaris_hms_sync_duration_seconds{operation}} — HMS call latency
 * </ul>
 */
@Startup
@ApplicationScoped
@Identifier("hms-reverse-sync")
public class HmsReverseSyncEventListener implements PolarisEventListener {

  private static final Logger LOG = LoggerFactory.getLogger(HmsReverseSyncEventListener.class);

  private static final String METRIC_EVENTS = "polaris.hms.sync.events";
  private static final String METRIC_SKIPPED = "polaris.hms.sync.skipped";
  private static final String METRIC_SUCCESSES = "polaris.hms.sync.successes";
  private static final String METRIC_ERRORS = "polaris.hms.sync.errors";
  private static final String METRIC_DURATION = "polaris.hms.sync.duration";
  private static final String METRIC_RETRIES = "polaris.hms.sync.retries";

  // A transient HMS blip used to drop the write entirely, which matters more than it
  // looks: HMS has no compare-and-set, so nothing notices the gap and the table stays
  // behind until something else happens to write to it.
  private static final int MAX_HMS_ATTEMPTS = 3;
  private static final long RETRY_BACKOFF_MS = 250;

  // Serializes reverse syncs per table within this JVM. syncToHms reads the table and
  // then alters it, and HMS gives no way to make that pair atomic, so two syncs for one
  // table can both read the same state and then write in either order -- leaving HMS on
  // the older one. Striped rather than a lock per table to bound memory across a catalog
  // with thousands of tables; a hash collision costs only brief contention.
  //
  // This does nothing across Polaris hosts, since writes to one table can arrive at any
  // of them. The race is narrowed, not removed, and polaris_hms_sync_check stays the
  // backstop for drift.
  private static final int LOCK_STRIPES = 64;

  private final Object[] tableLocks = newLockStripes();

  private static Object[] newLockStripes() {
    Object[] locks = new Object[LOCK_STRIPES];
    for (int i = 0; i < LOCK_STRIPES; i++) {
      locks[i] = new Object();
    }
    return locks;
  }

  private Object lockFor(String namespace, String tableName) {
    return tableLocks[Math.floorMod((namespace + "." + tableName).hashCode(), LOCK_STRIPES)];
  }

  @Inject
  @ConfigProperty(name = "polaris.hms-reverse-sync.enabled", defaultValue = "false")
  boolean enabled;

  @Inject
  @ConfigProperty(name = "polaris.hms-reverse-sync.hms-host", defaultValue = "localhost")
  String hmsHost;

  @Inject
  @ConfigProperty(name = "polaris.hms-reverse-sync.hms-port", defaultValue = "9083")
  int hmsPort;

  @Inject
  @ConfigProperty(name = "polaris.hms-reverse-sync.catalog", defaultValue = "liftoff_internal")
  String watchedCatalog;

  @Inject
  @ConfigProperty(
      name = "polaris.hms-reverse-sync.ignore-principal",
      defaultValue = "hms-sync")
  String ignorePrincipal;

  // Propagate DROP TABLE events back to HMS. Default false: HMS is the source of
  // truth; Polaris deletes (teardowns, POC experiments) must not remove HMS tables.
  @Inject
  @ConfigProperty(
      name = "polaris.hms-reverse-sync.propagate-deletes",
      defaultValue = "false")
  boolean propagateDeletes;

  @Inject
  @ConfigProperty(name = "polaris.hms-reverse-sync.hms-timeout-ms", defaultValue = "5000")
  int hmsTimeoutMs;

  // Identity this listener declares to HMS via set_ugi, so the HMS→Polaris forward
  // sync can recognize its own write-backs and skip them. Must match
  // polaris.sync.ignore-user in hive-site.xml, and requires
  // hive.metastore.execute.setugi=true on the metastore.
  @Inject
  @ConfigProperty(name = "polaris.hms-reverse-sync.hms-user", defaultValue = "polaris-sync")
  String hmsUser;

  @Inject
  Vertx vertx;

  @Inject
  MeterRegistry meterRegistry;

  @PostConstruct
  void initMetrics() {
    // Pre-register at zero so all time-series appear in Prometheus on startup
    for (String op : List.of("create_table", "register_table", "update_table", "drop_table")) {
      meterRegistry.counter(METRIC_EVENTS, "operation", op);
      meterRegistry.counter(METRIC_SUCCESSES, "operation", op);
      meterRegistry.counter(METRIC_ERRORS, "operation", op);
      meterRegistry.timer(METRIC_DURATION, "operation", op);
      meterRegistry.counter(METRIC_RETRIES, "operation", op);
    }
    for (String reason : List.of("echo", "propagate_deletes_off", "no_metadata_location")) {
      meterRegistry.counter(METRIC_SKIPPED, "reason", reason);
    }
  }

  // ── Event dispatch ───────────────────────────────────────────────────────

  @Override
  public void onEvent(PolarisEvent event) {
    String catalogName = event.attributes().get(EventAttributes.CATALOG_NAME).orElse(null);
    if (!shouldSync(catalogName)) return;
    if (isEchoEvent(event)) {
      meterRegistry.counter(METRIC_SKIPPED, "reason", "echo").increment();
      return;
    }

    switch (event.type()) {
      case AFTER_CREATE_TABLE -> {
        String ns = event.attributes().getRequired(EventAttributes.NAMESPACE).toString();
        String tbl = event.attributes().getRequired(EventAttributes.TABLE_NAME);
        TableMetadata meta = tableMetadataOf(event);
        String loc = meta == null ? null : meta.metadataFileLocation();
        LOG.info("onAfterCreateTable: {}.{} @ {}", ns, tbl, loc);
        meterRegistry.counter(METRIC_EVENTS, "operation", "create_table").increment();
        vertx.executeBlocking(() -> { syncToHms(ns, tbl, meta, false, "create_table"); return null; })
            .onFailure(err -> LOG.error("Unexpected error in HMS sync worker ({}.{})", ns, tbl, err));
      }
      case AFTER_REGISTER_TABLE -> {
        // registerTable is normally triggered by the HMS→Polaris forward sync;
        // echo detection handles that case, but we also sync non-echo registrations.
        String ns = event.attributes().getRequired(EventAttributes.NAMESPACE).toString();
        String tbl = event.attributes().getRequired(EventAttributes.TABLE_NAME);
        TableMetadata meta = tableMetadataOf(event);
        String loc = meta == null ? null : meta.metadataFileLocation();
        LOG.info("onAfterRegisterTable (non-echo): {}.{} @ {}", ns, tbl, loc);
        meterRegistry.counter(METRIC_EVENTS, "operation", "register_table").increment();
        vertx.executeBlocking(() -> { syncToHms(ns, tbl, meta, false, "register_table"); return null; })
            .onFailure(err -> LOG.error("Unexpected error in HMS sync worker ({}.{})", ns, tbl, err));
      }
      case AFTER_UPDATE_TABLE -> {
        String ns = event.attributes().getRequired(EventAttributes.NAMESPACE).toString();
        String tbl = event.attributes().getRequired(EventAttributes.TABLE_NAME);
        TableMetadata meta = tableMetadataOf(event);
        String loc = meta == null ? null : meta.metadataFileLocation();
        LOG.info("onAfterUpdateTable: {}.{} @ {}", ns, tbl, loc);
        meterRegistry.counter(METRIC_EVENTS, "operation", "update_table").increment();
        vertx.executeBlocking(() -> { syncToHms(ns, tbl, meta, true, "update_table"); return null; })
            .onFailure(err -> LOG.error("Unexpected error in HMS sync worker ({}.{})", ns, tbl, err));
      }
      case AFTER_DROP_TABLE -> {
        String ns = event.attributes().getRequired(EventAttributes.NAMESPACE).toString();
        String tbl = event.attributes().getRequired(EventAttributes.TABLE_NAME);
        if (!propagateDeletes) {
          LOG.warn("Skipping HMS drop for {}.{} — propagate-deletes=false; "
              + "set polaris.hms-reverse-sync.propagate-deletes=true to enable", ns, tbl);
          meterRegistry.counter(METRIC_SKIPPED, "reason", "propagate_deletes_off").increment();
          return;
        }
        LOG.info("onAfterDropTable: {}.{}", ns, tbl);
        meterRegistry.counter(METRIC_EVENTS, "operation", "drop_table").increment();
        vertx.executeBlocking(() -> { dropFromHms(ns, tbl); return null; })
            .onFailure(err -> LOG.error("Unexpected error in HMS drop worker ({}.{})", ns, tbl, err));
      }
      default -> {}
    }
  }

  // ── Echo detection ───────────────────────────────────────────────────────

  private boolean isEchoEvent(PolarisEvent event) {
    String principal = event.metadata().user().map(PolarisPrincipal::getName).orElse(null);
    if (ignorePrincipal.equals(principal)) {
      LOG.debug("Skipping echo event from principal '{}'", principal);
      return true;
    }
    return false;
  }

  private boolean shouldSync(String catalogName) {
    return enabled && watchedCatalog.equals(catalogName);
  }

  // ── HMS Thrift operations ────────────────────────────────────────────────

  private void syncToHms(
      String namespace, String tableName, TableMetadata metadata, boolean isAlter,
      String operation) {
    String metadataLocation = metadata == null ? null : metadata.metadataFileLocation();
    if (metadataLocation == null || metadataLocation.isEmpty()) {
      LOG.warn("Skipping HMS sync for {}.{} — no metadata_location", namespace, tableName);
      meterRegistry.counter(METRIC_SKIPPED, "reason", "no_metadata_location").increment();
      return;
    }
    Timer.Sample sample = Timer.start(meterRegistry);
    try {
      for (int attempt = 1; attempt <= MAX_HMS_ATTEMPTS; attempt++) {
        try {
          // Locked per attempt rather than around the whole loop: holding a stripe
          // across backoff and a 5s socket timeout would stall every other table
          // hashing to it.
          synchronized (lockFor(namespace, tableName)) {
            attemptSync(namespace, tableName, metadata, isAlter);
          }
          meterRegistry.counter(METRIC_SUCCESSES, "operation", operation).increment();
          return;
        } catch (TTransportException e) {
          // Connection refused, socket timeout, HMS restarting: worth another go.
          // Logical failures fall through to the catch below and are not retried.
          if (attempt == MAX_HMS_ATTEMPTS) {
            LOG.error("Failed HMS sync for {}.{} after {} attempts: {}",
                namespace, tableName, MAX_HMS_ATTEMPTS, e.getMessage(), e);
            meterRegistry.counter(METRIC_ERRORS, "operation", operation).increment();
            return;
          }
          LOG.warn("HMS sync for {}.{} failed (attempt {}/{}), retrying: {}",
              namespace, tableName, attempt, MAX_HMS_ATTEMPTS, e.getMessage());
          meterRegistry.counter(METRIC_RETRIES, "operation", operation).increment();
          Thread.sleep(RETRY_BACKOFF_MS * attempt);
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.warn("Interrupted syncing {}.{} to HMS", namespace, tableName);
      meterRegistry.counter(METRIC_ERRORS, "operation", operation).increment();
    } catch (Exception e) {
      LOG.error("Failed HMS sync for {}.{}: {}", namespace, tableName, e.getMessage(), e);
      meterRegistry.counter(METRIC_ERRORS, "operation", operation).increment();
    } finally {
      sample.stop(meterRegistry.timer(METRIC_DURATION, "operation", operation));
    }
  }

  /** One connect-and-write attempt. Callers hold the table's lock and handle retries. */
  private void attemptSync(
      String namespace, String tableName, TableMetadata metadata, boolean isAlter)
      throws Exception {
    String metadataLocation = metadata.metadataFileLocation();
    TSocket transport = null;
    try {
      transport = new TSocket(hmsHost, hmsPort, hmsTimeoutMs);
      transport.open();
      ThriftHiveMetastore.Client client =
          new ThriftHiveMetastore.Client(new TBinaryProtocol(transport));
      client.set_ugi(hmsUser, Collections.emptyList());

      if (isAlter) {
        try {
          Table existing = client.get_table(namespace, tableName);
          applyPointerUpdate(existing, metadataLocation);
          // DO_NOT_UPDATE_STATS stops HMS recomputing numFiles and totalSize, which it
          // does by listing the storage location -- a directory listing per sync, for
          // numbers that mean nothing on an Iceberg table. Iceberg's own writer sets it
          // for the same reason.
          EnvironmentContext ctx = new EnvironmentContext();
          ctx.putToProperties(StatsSetupConst.DO_NOT_UPDATE_STATS, "true");
          client.alter_table_with_environment_context(namespace, tableName, existing, ctx);
          LOG.info("HMS alter_table {}.{} OK", namespace, tableName);
        } catch (NoSuchObjectException e) {
          LOG.info("{}.{} not in HMS, creating instead", namespace, tableName);
          ensureDatabaseAndCreateTable(client, namespace, tableName, metadata);
        }
      } else {
        // Idempotent: drop first (ignore 404), then create
        try {
          client.drop_table(namespace, tableName, false);
        } catch (Exception ignored) {
        }
        ensureDatabaseAndCreateTable(client, namespace, tableName, metadata);
      }
    } finally {
      if (transport != null && transport.isOpen()) transport.close();
    }
  }

  /**
   * Extracts the table metadata an event carries, whichever attribute it used.
   *
   * <p>Create and register attach {@code LOAD_TABLE_RESPONSE}; update attaches
   * {@code TABLE_METADATA}, including each per-table event that commitTransaction emits.
   * Reading only one of the two is how updates came to be silently dropped, so both are
   * checked here in one place rather than separately in each branch.
   */
  private static TableMetadata tableMetadataOf(PolarisEvent event) {
    Optional<TableMetadata> direct = event.attributes().get(EventAttributes.TABLE_METADATA);
    if (direct.isPresent()) {
      return direct.get();
    }
    return event
        .attributes()
        .get(EventAttributes.LOAD_TABLE_RESPONSE)
        .map(LoadTableResponse::tableMetadata)
        .orElse(null);
  }

  /** Strips the last path segment, used to place a database beside its tables. */
  private static String parentOf(String location) {
    if (location == null) {
      return null;
    }
    String trimmed =
        location.endsWith("/") ? location.substring(0, location.length() - 1) : location;
    int slash = trimmed.lastIndexOf('/');
    return slash > 0 ? trimmed.substring(0, slash) : trimmed;
  }

  /**
   * Repoints an existing HMS table at new Iceberg metadata.
   *
   * <p>Mirrors what {@code HiveTableOperations} writes on a commit, so a synced table is
   * indistinguishable from one an Iceberg client wrote directly. That reference
   * implementation is the thing to check against if HMS rows ever look wrong:
   *
   * <ul>
   *   <li>{@code metadata_location} — the new pointer
   *   <li>{@code previous_metadata_location} — the file just superseded. Without it the row
   *       is internally inconsistent: the pointer moves while the breadcrumb stays wherever
   *       it was, which is a state Iceberg would never produce.
   *   <li>{@code COLUMN_STATS_ACCURATE} removed — a table migrated from Hive can carry this
   *       from an old ANALYZE, and leaving it in place asserts that stats are accurate for a
   *       snapshot that no longer exists. Iceberg removes it rather than setting it false.
   * </ul>
   *
   * <p>Deliberately does not maintain {@code numRows} or {@code totalSize}. They are
   * meaningless for an Iceberg table, whose counts live in the manifests, and no reader
   * benefits from keeping two systems' idea of them in agreement.
   */
  private static void applyPointerUpdate(Table table, String metadataLocation) {
    Map<String, String> params = table.getParameters();
    String previous = params.get(BaseMetastoreTableOperations.METADATA_LOCATION_PROP);
    params.put(BaseMetastoreTableOperations.METADATA_LOCATION_PROP, metadataLocation);
    if (previous != null && !previous.equals(metadataLocation)) {
      params.put(BaseMetastoreTableOperations.PREVIOUS_METADATA_LOCATION_PROP, previous);
    }
    params.remove(StatsSetupConst.COLUMN_STATS_ACCURATE);
  }

  private void ensureDatabaseAndCreateTable(
      ThriftHiveMetastore.Client client,
      String namespace,
      String tableName,
      TableMetadata metadata)
      throws Exception {
    String metadataLocation = metadata.metadataFileLocation();
    String tableLocation = metadata.location();
    try {
      client.get_database(namespace);
    } catch (NoSuchObjectException e) {
      Database db = new Database();
      db.setName(namespace);
      // Derived from the table's own location rather than a fixed warehouse path: this
      // listener has no idea what the deployment's warehouse root is, and a wrong value
      // here is what HMS hands to anything doing location-based logic.
      db.setLocationUri(parentOf(tableLocation));
      db.setParameters(Collections.emptyMap());
      client.create_database(db);
      LOG.info("HMS create_database {} at {} OK", namespace, db.getLocationUri());
    }

    Map<String, String> params = new HashMap<>();
    params.put("table_type", "ICEBERG");
    params.put("metadata_location", metadataLocation);
    params.put("EXTERNAL", "TRUE");

    SerDeInfo serDeInfo = new SerDeInfo();
    serDeInfo.setSerializationLib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
    serDeInfo.setParameters(Collections.emptyMap());

    StorageDescriptor sd = new StorageDescriptor();
    sd.setCols(new ArrayList<FieldSchema>());
    sd.setLocation(tableLocation);
    sd.setInputFormat("org.apache.hadoop.mapred.TextInputFormat");
    sd.setOutputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat");
    sd.setSerdeInfo(serDeInfo);
    sd.setParameters(Collections.emptyMap());

    Table table = new Table();
    table.setTableName(tableName);
    table.setDbName(namespace);
    table.setTableType("EXTERNAL_TABLE");
    table.setSd(sd);
    table.setParameters(params);

    client.create_table(table);
    LOG.info("HMS create_table {}.{} OK", namespace, tableName);
  }

  private void dropFromHms(String namespace, String tableName) {
    Timer.Sample sample = Timer.start(meterRegistry);
    TSocket transport = null;
    try {
      transport = new TSocket(hmsHost, hmsPort, hmsTimeoutMs);
      transport.open();
      ThriftHiveMetastore.Client client =
          new ThriftHiveMetastore.Client(new TBinaryProtocol(transport));
      client.set_ugi(hmsUser, Collections.emptyList());
      client.drop_table(namespace, tableName, false);
      LOG.info("HMS drop_table {}.{} OK", namespace, tableName);
      meterRegistry.counter(METRIC_SUCCESSES, "operation", "drop_table").increment();
    } catch (Exception e) {
      LOG.warn("HMS drop_table {}.{} — {}", namespace, tableName, e.getMessage());
      meterRegistry.counter(METRIC_ERRORS, "operation", "drop_table").increment();
    } finally {
      if (transport != null && transport.isOpen()) transport.close();
      sample.stop(meterRegistry.timer(METRIC_DURATION, "operation", "drop_table"));
    }
  }


}
