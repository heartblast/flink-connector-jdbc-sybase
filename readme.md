# Flink JDBC Connector – Sybase ASE

`flink-connector-jdbc-sybase` is a custom JDBC connector plugin for **Apache Flink** that enables seamless integration with **Sybase ASE** databases.  
It provides a dedicated SQL dialect and type converters to handle Sybase-specific data types such as **unsigned TINYINT** safely and efficiently.

## Features

- **Sybase ASE-specific SQL Dialect** for compatibility with Flink’s JDBC connector.
- Safe and correct type conversion (e.g., TINYINT 0–255 mapped to `Short` to avoid sign issues).
- Built on top of `AbstractDialectConverter` for maintainable and extensible design.
- Defensive programming with null checks, range validation, and driver type flexibility.

## Quick Start

### Prerequisites
- Java 11 or higher
- Apache Flink (version compatibility recommended to match your Flink deployment)
- Maven 3.x
- Sybase JDBC Driver (`jconn4.jar` or compatible)

### Build

```bash
git clone https://github.com/heartblast/flink-connector-jdbc-sybase.git
cd flink-connector-jdbc-sybase
mvn clean install
````

### Example Usage (Flink SQL)

```sql
CREATE TABLE sybase_source (
  id INT,
  tiny_col SMALLINT, -- mapped from Sybase TINYINT
  ...
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:sybase:Tds://<host>:<port>/<database>',
  'table-name' = '<table>',
  'driver' = 'com.sybase.jdbc4.jdbc.SybDriver',
  'username' = '<user>',
  'password' = '<password>'
);
```

> **Note:** The `SybaseDialectConverter` ensures unsigned TINYINT values are stored as `Short` (0–255) in Flink’s internal representation.

## Architecture

* **`SybaseDialectConverter`**
  Extends Flink’s `AbstractDialectConverter` to override deserialization logic for Sybase ASE.
* **Unsigned TINYINT Handling**
  Safely maps JDBC numeric/byte/string values to Flink-compatible numeric types without losing sign correctness.
* **Extensibility**
  Easily add converters for other Sybase-specific types if needed.

## Testing

Recommended test coverage includes:

* **Valid cases:** `0`, `127`, `128`, `255`
* **Invalid cases:** `-1`, `256`, invalid string, empty byte array
* **Driver variations:** Numeric (`Short`, `Integer`), `byte[]`, and `String` inputs

## Contributing

Contributions are welcome!
Feel free to open an **Issue** or submit a **Pull Request** for bug fixes, improvements, or additional Sybase type support.

## License

This project is licensed under the Apache License 2.0 – see the [LICENSE](LICENSE) file for details.

```

