// SPDX-License-Identifier: Apache-2.0

use crate::semconv;
use std::collections::HashMap;
use std::sync::LazyLock;

pub const DB_TYPE_SQL: &str = "sql";
pub const DB_TYPE_CASSANDRA: &str = "cassandra";
pub const DB_TYPE_REDIS: &str = "redis";
pub const DB_TYPE_MEMCACHED: &str = "memcached";
pub const DB_TYPE_MONGODB: &str = "mongodb";
pub const DB_TYPE_ELASTICSEARCH: &str = "elasticsearch";
pub const DB_TYPE_OPENSEARCH: &str = "opensearch";
pub const DB_TYPE_DB: &str = "db";

/// Classification of DB systems to their general type, as defined by Datadog. These
/// are used to initialize the span type.
///
/// This is lazily initialized once, upon use.
pub static DB_TYPES: LazyLock<HashMap<&'static str, &'static str>> = LazyLock::new(|| {
    let mut m = HashMap::new();

    // SQL database systems
    m.insert(semconv::db_system::OTHER_SQL, DB_TYPE_SQL);
    m.insert(semconv::db_system::MSSQL, DB_TYPE_SQL);
    m.insert(semconv::db_system::MYSQL, DB_TYPE_SQL);
    m.insert(semconv::db_system::ORACLE, DB_TYPE_SQL);
    m.insert(semconv::db_system::DB2, DB_TYPE_SQL);
    m.insert(semconv::db_system::POSTGRESQL, DB_TYPE_SQL);
    m.insert(semconv::db_system::REDSHIFT, DB_TYPE_SQL);
    m.insert(semconv::db_system::CLOUDSCAPE, DB_TYPE_SQL);
    m.insert(semconv::db_system::HSQLDB, DB_TYPE_SQL);
    m.insert(semconv::db_system::MAXDB, DB_TYPE_SQL);
    m.insert(semconv::db_system::INGRES, DB_TYPE_SQL);
    m.insert(semconv::db_system::FIRST_SQL, DB_TYPE_SQL);
    m.insert(semconv::db_system::EDB, DB_TYPE_SQL);
    m.insert(semconv::db_system::CACHE, DB_TYPE_SQL);
    m.insert(semconv::db_system::FIREBIRD, DB_TYPE_SQL);
    m.insert(semconv::db_system::DERBY, DB_TYPE_SQL);
    m.insert(semconv::db_system::INFORMIX, DB_TYPE_SQL);
    m.insert(semconv::db_system::MARIADB, DB_TYPE_SQL);
    m.insert(semconv::db_system::SQLITE, DB_TYPE_SQL);
    m.insert(semconv::db_system::SYBASE, DB_TYPE_SQL);
    m.insert(semconv::db_system::TERADATA, DB_TYPE_SQL);
    m.insert(semconv::db_system::VERTICA, DB_TYPE_SQL);
    m.insert(semconv::db_system::H2, DB_TYPE_SQL);
    m.insert(semconv::db_system::COLDFUSION, DB_TYPE_SQL);
    m.insert(semconv::db_system::COCKROACHDB, DB_TYPE_SQL);
    m.insert(semconv::db_system::PROGRESS, DB_TYPE_SQL);
    m.insert(semconv::db_system::HANA, DB_TYPE_SQL);
    m.insert(semconv::db_system::ADABAS, DB_TYPE_SQL);
    m.insert(semconv::db_system::FILEMAKER, DB_TYPE_SQL);
    m.insert(semconv::db_system::INSTANT_DB, DB_TYPE_SQL);
    m.insert(semconv::db_system::INTERBASE, DB_TYPE_SQL);
    m.insert(semconv::db_system::NETEZZA, DB_TYPE_SQL);
    m.insert(semconv::db_system::PERVASIVE, DB_TYPE_SQL);
    m.insert(semconv::db_system::POINTBASE, DB_TYPE_SQL);
    m.insert(semconv::db_system::CLICKHOUSE, DB_TYPE_SQL);

    // Specific database systems
    m.insert(semconv::db_system::CASSANDRA, DB_TYPE_CASSANDRA);
    m.insert(semconv::db_system::REDIS, DB_TYPE_REDIS);
    m.insert(semconv::db_system::MEMCACHED, DB_TYPE_MEMCACHED);
    m.insert(semconv::db_system::MONGODB, DB_TYPE_MONGODB);
    m.insert(semconv::db_system::ELASTICSEARCH, DB_TYPE_ELASTICSEARCH);
    m.insert(semconv::db_system::OPENSEARCH, DB_TYPE_OPENSEARCH);

    // Generic database types
    m.insert(semconv::db_system::HIVE, DB_TYPE_DB);
    m.insert(semconv::db_system::HBASE, DB_TYPE_DB);
    m.insert(semconv::db_system::NEO4J, DB_TYPE_DB);
    m.insert(semconv::db_system::COUCHBASE, DB_TYPE_DB);
    m.insert(semconv::db_system::COUCHDB, DB_TYPE_DB);
    m.insert(semconv::db_system::COSMOS_DB, DB_TYPE_DB);
    m.insert(semconv::db_system::DYNAMODB, DB_TYPE_DB);
    m.insert(semconv::db_system::GEODE, DB_TYPE_DB);

    m
});
