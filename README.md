# HTAPsim

A HTAP simulation - just completed with insert operations only
This uses a bgworker to perform logical decoding from WAL segments using SQL statements & output plugin (test_decoding) to fetch decoded changes.
These decodes will be used to insert on columnstore (citus) using SPI - used for SQL statement execution

Reference: https://www.postgresql.org/docs/17/logicaldecoding-example.html
