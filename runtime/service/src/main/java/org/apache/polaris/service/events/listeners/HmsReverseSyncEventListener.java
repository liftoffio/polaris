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
 */
package org.apache.polaris.service.events.listeners;

import io.smallrye.common.annotation.Identifier;
import io.vertx.core.Vertx;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.service.events.EventAttributes;
import org.apache.polaris.service.events.PolarisEvent;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CDI event listener that mirrors table create/update/drop events from the
 * Polaris INTERNAL catalog back to a Hive Metastore (HMS) via Thrift, so that
 * HMS-based readers see the updated metadata_location.
 *
 * <p>Echo detection: events originating from the configured
 * {@code ignore-principal} (default: {@code hms-sync}) are skipped — those
 * come from the HMS→Polaris forward sync listener and would otherwise cause
 * an infinite loop.
 */
@ApplicationScoped
@Identifier("hms-reverse-sync")
public class HmsReverseSyncEventListener implements PolarisEventListener {

  private static final Logger LOG = LoggerFactory.getLogger(HmsReverseSyncEventListener.class);

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

  @Inject
  Vertx vertx;

  // ── Event dispatch ───────────────────────────────────────────────────────

  @Override
  public void onEvent(PolarisEvent event) {
    String catalogName = event.attributes().get(EventAttributes.CATALOG_NAME).orElse(null);
    if (!shouldSync(catalogName) || isEchoEvent(event)) return;

    switch (event.type()) {
      case AFTER_CREATE_TABLE -> {
        String ns = event.attributes().getRequired(EventAttributes.NAMESPACE).toString();
        String tbl = event.attributes().getRequired(EventAttributes.TABLE_NAME);
        String loc =
            event
                .attributes()
                .get(EventAttributes.LOAD_TABLE_RESPONSE)
                .map(HmsReverseSyncEventListener::metadataLocation)
                .orElse(null);
        LOG.info("onAfterCreateTable: {}.{} @ {}", ns, tbl, loc);
        vertx.executeBlocking(() -> { syncToHms(ns, tbl, loc, false); return null; })
            .onFailure(err -> LOG.error("Unexpected error in HMS sync worker ({}.{})", ns, tbl, err));
      }
      case AFTER_REGISTER_TABLE -> {
        // registerTable is normally triggered by the HMS→Polaris forward sync;
        // echo detection handles that case, but we also sync non-echo registrations.
        String ns = event.attributes().getRequired(EventAttributes.NAMESPACE).toString();
        String tbl = event.attributes().getRequired(EventAttributes.TABLE_NAME);
        String loc =
            event
                .attributes()
                .get(EventAttributes.LOAD_TABLE_RESPONSE)
                .map(HmsReverseSyncEventListener::metadataLocation)
                .orElse(null);
        LOG.info("onAfterRegisterTable (non-echo): {}.{} @ {}", ns, tbl, loc);
        vertx.executeBlocking(() -> { syncToHms(ns, tbl, loc, false); return null; })
            .onFailure(err -> LOG.error("Unexpected error in HMS sync worker ({}.{})", ns, tbl, err));
      }
      case AFTER_UPDATE_TABLE -> {
        String ns = event.attributes().getRequired(EventAttributes.NAMESPACE).toString();
        String tbl = event.attributes().getRequired(EventAttributes.TABLE_NAME);
        String loc =
            event
                .attributes()
                .get(EventAttributes.LOAD_TABLE_RESPONSE)
                .map(HmsReverseSyncEventListener::metadataLocation)
                .orElse(null);
        LOG.info("onAfterUpdateTable: {}.{} @ {}", ns, tbl, loc);
        vertx.executeBlocking(() -> { syncToHms(ns, tbl, loc, true); return null; })
            .onFailure(err -> LOG.error("Unexpected error in HMS sync worker ({}.{})", ns, tbl, err));
      }
      case AFTER_DROP_TABLE -> {
        String ns = event.attributes().getRequired(EventAttributes.NAMESPACE).toString();
        String tbl = event.attributes().getRequired(EventAttributes.TABLE_NAME);
        if (!propagateDeletes) {
          LOG.warn("Skipping HMS drop for {}.{} — propagate-deletes=false; "
              + "set polaris.hms-reverse-sync.propagate-deletes=true to enable", ns, tbl);
          return;
        }
        LOG.info("onAfterDropTable: {}.{}", ns, tbl);
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
      String namespace, String tableName, String metadataLocation, boolean isAlter) {
    if (metadataLocation == null || metadataLocation.isEmpty()) {
      LOG.warn("Skipping HMS sync for {}.{} — no metadata_location", namespace, tableName);
      return;
    }
    TSocket transport = null;
    try {
      transport = new TSocket(hmsHost, hmsPort, hmsTimeoutMs);
      transport.open();
      ThriftHiveMetastore.Client client =
          new ThriftHiveMetastore.Client(new TBinaryProtocol(transport));

      if (isAlter) {
        try {
          Table existing = client.get_table(namespace, tableName);
          existing.getParameters().put("metadata_location", metadataLocation);
          client.alter_table(namespace, tableName, existing);
          LOG.info("HMS alter_table {}.{} OK", namespace, tableName);
        } catch (NoSuchObjectException e) {
          LOG.info("{}.{} not in HMS, creating instead", namespace, tableName);
          ensureDatabaseAndCreateTable(client, namespace, tableName, metadataLocation);
        }
      } else {
        // Idempotent: drop first (ignore 404), then create
        try {
          client.drop_table(namespace, tableName, false);
        } catch (Exception ignored) {
        }
        ensureDatabaseAndCreateTable(client, namespace, tableName, metadataLocation);
      }
    } catch (Exception e) {
      LOG.error("Failed HMS sync for {}.{}: {}", namespace, tableName, e.getMessage(), e);
    } finally {
      if (transport != null && transport.isOpen()) transport.close();
    }
  }

  private void ensureDatabaseAndCreateTable(
      ThriftHiveMetastore.Client client,
      String namespace,
      String tableName,
      String metadataLocation)
      throws Exception {
    try {
      client.get_database(namespace);
    } catch (NoSuchObjectException e) {
      Database db = new Database();
      db.setName(namespace);
      db.setLocationUri("/tmp/hive/warehouse/" + namespace);
      db.setParameters(Collections.emptyMap());
      client.create_database(db);
      LOG.info("HMS create_database {} OK", namespace);
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
    sd.setLocation("/tmp/hive/warehouse/" + namespace + "/" + tableName);
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
    TSocket transport = null;
    try {
      transport = new TSocket(hmsHost, hmsPort, hmsTimeoutMs);
      transport.open();
      ThriftHiveMetastore.Client client =
          new ThriftHiveMetastore.Client(new TBinaryProtocol(transport));
      client.drop_table(namespace, tableName, false);
      LOG.info("HMS drop_table {}.{} OK", namespace, tableName);
    } catch (Exception e) {
      LOG.warn("HMS drop_table {}.{} — {}", namespace, tableName, e.getMessage());
    } finally {
      if (transport != null && transport.isOpen()) transport.close();
    }
  }

  private static String metadataLocation(LoadTableResponse response) {
    if (response == null) return null;
    TableMetadata meta = response.tableMetadata();
    return meta != null ? meta.metadataFileLocation() : null;
  }
}
