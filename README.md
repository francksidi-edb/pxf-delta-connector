# PXF Delta Connector

## Overview
This is the first release of the Delta Connector for PXF. The next phase will include:
- Vectorization
- Predicate/aggregate/joins pushdown

---

## Installation Steps

### 1. Prerequisites
- **Greenplum Database** and **PXF Extension** must be installed.

### 2. Update PXF Profiles
Update the `pxf-profiles.xml` file located at `$PXF_BASE/conf` to include the Delta Connector profile.

```xml
<profiles>
    <profile>
        <name>delta</name>
        <plugins>
            <fragmenter>com.example.pxf.DeltaTableFragmenter</fragmenter>
            <accessor>com.example.pxf.DeltaTableAccessor</accessor>
            <resolver>com.example.pxf.DeltaTableResolver</resolver>
        </plugins>
    </profile>
</profiles>

3. Install Maven

Ensure Maven is installed on your system. You can check your Maven version with:

mvn --version

4. Compile and Package the Connector

Navigate to the pxf-delta-connector directory and run:

mvn clean package

If successful, the package will be built:

[INFO] --- maven-jar-plugin:3.2.0:jar (default-jar) @ pxf-delta-connector ---
[INFO] Building jar: /home/gpadmin/pxf/pxf-delta-connector/target/pxf-delta-connector-1.0-SNAPSHOT.jar

5. Deploy the Jar

Use the deployment script to stop PXF, deploy the connector, and restart PXF:

./deploy.sh

The script will:

    Stop PXF
    Copy the package
    Clear logs
    Restart PXF

Example output:

Stopping PXF on coordinator host and 0 segment hosts...
PXF stopped successfully on 1 out of 1 host
Starting PXF on coordinator host and 0 segment hosts...
PXF started successfully on 1 out of 1 host

6. Create an External Table in Greenplum

Connect to Greenplum and create an external table.

DROP EXTERNAL TABLE IF EXISTS ext_transactions_d;

CREATE EXTERNAL TABLE ext_transactions_d (
    transaction_id   BIGINT,
    user_id          BIGINT,
    merchant_id      BIGINT,
    product_id       BIGINT,
    transaction_date VARCHAR(10), -- Treat as VARCHAR to avoid type mismatch
    amount           NUMERIC(10,2),
    loyalty_points   INTEGER,
    payment_method   VARCHAR(50)
)
LOCATION ('pxf:///mnt/data/parquet/transactions?PROFILE=delta')
FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');

7. Query the External Table

You can query the external table as follows:

SELECT * FROM ext_transactions_d LIMIT 10;

Example output:

 transaction_id | user_id | merchant_id | product_id | transaction_date |  amount  | loyalty_points | payment_method 
----------------+---------+-------------+------------+------------------+----------+----------------+----------------
      368124443 | 7671444 |      502842 |     211955 | 2024-02-07       | 44263.00 |             82 | Mobile Pay
      368190215 | 5187904 |      589170 |     234751 | 2024-02-24       | 71045.00 |              3 | Mobile Pay
      368192222 | 6893445 |      270994 |     352707 | 2024-02-02       | 55530.00 |             55 | Debit Card
...

Future Improvements

    Add vectorization for enhanced performance.
    Implement predicate and aggregate pushdown.
    Support join pushdowns.
