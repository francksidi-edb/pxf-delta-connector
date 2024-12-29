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
```

### 3. Install Maven
Ensure Maven is installed on your system. You can check your Maven installation and

### 4. Compile and Package the Project
Use Maven to clean and build the project into a JAR file.

```bash
gpadmin@gpmaster:~/pxf/pxf-delta-connector$ mvn clean package
### 4. Compile and Package the Project
Use Maven to clean and build the project into a JAR file.

[INFO] --- maven-jar-plugin:3.2.0:jar (default-jar) @ pxf-delta-connector ---
[INFO] Building jar: /home/gpadmin/pxf/pxf-delta-connector/target/pxf-delta-connector-1.0-SNAPSHOT.jar

```
### 5. Deploy the JAR File

To deploy the JAR file, use the `deploy.sh` script. This script will:

- Stop PXF.
- Copy the compiled package.
- Clear all PXF logs.
- Restart PXF.

Run the following command:

```bash
./deploy.sh

Stopping PXF on coordinator host and 0 segment hosts...
PXF stopped successfully on 1 out of 1 host
Starting PXF on coordinator host and 0 segment hosts...
PXF started successfully on 1 out of 1 host

```
### Step 6 - Connect to Greenplum and Create an External Table

1. Drop the external table if it already exists:
   ```sql
   warehouse=# DROP EXTERNAL TABLE ext_transactions_d;
   ```

warehouse=# CREATE EXTERNAL TABLE ext_transactions_d (
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

```
warehouse=# SELECT * FROM ext_transactions_d LIMIT 10;

 transaction_id | user_id | merchant_id | product_id | transaction_date |  amount  | loyalty_points | payment_method 
----------------+---------+-------------+------------+------------------+----------+----------------+----------------
      368124443 | 7671444 |      502842 |     211955 | 2024-02-07       | 44263.00 |             82 | Mobile Pay
      368190215 | 5187904 |      589170 |     234751 | 2024-02-24       | 71045.00 |              3 | Mobile Pay
      368192222 | 6893445 |      270994 |     352707 | 2024-02-02       | 55530.00 |             55 | Debit Card
      368095715 | 8686145 |      599271 |      31717 | 2024-02-10       | 19731.00 |             65 | Credit Card
      368095971 | 1984538 |      198938 |     395405 | 2024-02-07       | 82284.00 |             77 | Mobile Pay
      368072689 | 7294881 |      822620 |      15519 | 2024-02-19       | 88835.00 |             89 | Credit Card
      368074678 | 3514583 |      175076 |     380181 | 2024-02-26       |  2392.00 |             68 | Credit Card
      368146928 | 2506668 |      309160 |     446281 | 2024-02-15       | 19122.00 |             76 | Debit Card
      368219332 | 4141167 |      228490 |      21146 | 2024-02-04       | 32496.00 |             13 | Credit Card
      368219537 | 9928407 |      972353 |     299168 | 2024-02-17       | 52130.00 |             39 | Debit Card



