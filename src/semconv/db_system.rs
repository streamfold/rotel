// SPDX-License-Identifier: Apache-2.0

// Taken from: https://github.com/open-telemetry/opentelemetry-collector/blob/semconv/v0.118.0/semconv/v1.6.1/generated_trace.go

// Some other SQL database. Fallback only. See notes
pub const OTHER_SQL: &str = "other_sql";
// Microsoft SQL Server
pub const MSSQL: &str = "mssql";
// MySQL
pub const MYSQL: &str = "mysql";
// Oracle Database
pub const ORACLE: &str = "oracle";
// IBM DB2
pub const DB2: &str = "db2";
// PostgreSQL
pub const POSTGRESQL: &str = "postgresql";
// Amazon Redshift
pub const REDSHIFT: &str = "redshift";
// Apache Hive
pub const HIVE: &str = "hive";
// Cloudscape
pub const CLOUDSCAPE: &str = "cloudscape";
// HyperSQL DataBase
pub const HSQLDB: &str = "hsqldb";
// Progress Database
pub const PROGRESS: &str = "progress";
// SAP MaxDB
pub const MAXDB: &str = "maxdb";
// SAP HANA
pub const HANA: &str = "hanadb";
// Ingres
pub const INGRES: &str = "ingres";
// FirstSQL
pub const FIRST_SQL: &str = "firstsql";
// EnterpriseDB
pub const EDB: &str = "edb";
// InterSystems Caché
pub const CACHE: &str = "cache";
// Adabas (Adaptable Database System)
pub const ADABAS: &str = "adabas";
// Firebird
pub const FIREBIRD: &str = "firebird";
// Apache Derby
pub const DERBY: &str = "derby";
// FileMaker
pub const FILEMAKER: &str = "filemaker";
// Informix
pub const INFORMIX: &str = "informix";
// InstantDB
pub const INSTANT_DB: &str = "instantdb";
// InterBase
pub const INTERBASE: &str = "interbase";
// MariaDB
pub const MARIADB: &str = "mariadb";
// Netezza
pub const NETEZZA: &str = "netezza";
// Pervasive PSQL
pub const PERVASIVE: &str = "pervasive";
// PointBase
pub const POINTBASE: &str = "pointbase";
// SQLite
pub const SQLITE: &str = "sqlite";
// Sybase
pub const SYBASE: &str = "sybase";
// Teradata
pub const TERADATA: &str = "teradata";
// Vertica
pub const VERTICA: &str = "vertica";
// H2
pub const H2: &str = "h2";
// ColdFusion IMQ
pub const COLDFUSION: &str = "coldfusion";
// Apache Cassandra
pub const CASSANDRA: &str = "cassandra";
// Apache HBase
pub const HBASE: &str = "hbase";
// MongoDB
pub const MONGODB: &str = "mongodb";
// Redis
pub const REDIS: &str = "redis";
// Couchbase
pub const COUCHBASE: &str = "couchbase";
// CouchDB
pub const COUCHDB: &str = "couchdb";
// Microsoft Azure Cosmos DB
pub const COSMOS_DB: &str = "cosmosdb";
// Amazon DynamoDB
pub const DYNAMODB: &str = "dynamodb";
// Neo4j
pub const NEO4J: &str = "neo4j";
// Apache Geode
pub const GEODE: &str = "geode";
// Elasticsearch
pub const ELASTICSEARCH: &str = "elasticsearch";
// Memcached
pub const MEMCACHED: &str = "memcached";
// CockroachDB
pub const COCKROACHDB: &str = "cockroachdb";

// These are added in newer semconv versions? (need to check version)
// Clickhouse
pub const CLICKHOUSE: &str = "clickhouse";
pub const OPENSEARCH: &str = "opensearch";
