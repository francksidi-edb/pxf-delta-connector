# pxf-delta-connector
## First release of the Delta Connector. Next phase will be to include Vectorization and predicate/aggregate/joins pushdown

1 - Greenplum and PXF Extension need to be installed 

2 - Inside pxf-profiles.xml in $PXF_BASE/conf add the following lines


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



3 - Maveen need to be installed 

gpadmin@gpmaster:~/pxf/conf$ mvn --version 
Apache Maven 3.8.7
Maven home: /usr/share/maven
Java version: 17.0.13, vendor: Debian, runtime: /usr/lib/jvm/java-17-openjdk-amd64
Default locale: en, platform encoding: UTF-8
OS name: "linux", version: "6.1.0-23-cloud-amd64", arch: "amd64", family: "unix"
gpadmin@gpmaster:~/pxf/conf$ 


4 - to compile and package 
gpadmin@gpmaster:~/pxf/pxf-delta-connector$ mvn clean package 

[INFO] 
[INFO] ------------------< com.example:pxf-delta-connector >-------------------
[INFO] Building pxf-delta-connector 1.0-SNAPSHOT
[INFO] --------------------------------[ jar ]---------------------------------
[INFO] 
[INFO] --- maven-clean-plugin:2.5:clean (default-clean) @ pxf-delta-connector ---
[INFO] Deleting /home/gpadmin/pxf/pxf-delta-connector/target
[INFO] 
[INFO] --- maven-resources-plugin:2.6:resources (default-resources) @ pxf-delta-connector ---
[WARNING] Using platform encoding (UTF-8 actually) to copy filtered resources, i.e. build is platform dependent!
[INFO] skip non existing resourceDirectory /home/gpadmin/pxf/pxf-delta-connector/src/main/resources
[INFO] 
[INFO] --- maven-compiler-plugin:3.8.1:compile (default-compile) @ pxf-delta-connector ---
[INFO] Changes detected - recompiling the module!
[WARNING] File encoding has not been set, using platform encoding UTF-8, i.e. build is platform dependent!
[INFO] Compiling 5 source files to /home/gpadmin/pxf/pxf-delta-connector/target/classes
[INFO] 
[INFO] --- maven-resources-plugin:2.6:testResources (default-testResources) @ pxf-delta-connector ---
[WARNING] Using platform encoding (UTF-8 actually) to copy filtered resources, i.e. build is platform dependent!
[INFO] skip non existing resourceDirectory /home/gpadmin/pxf/pxf-delta-connector/src/test/resources
[INFO] 
[INFO] --- maven-compiler-plugin:3.8.1:testCompile (default-testCompile) @ pxf-delta-connector ---
[INFO] No sources to compile
[INFO] 
[INFO] --- maven-surefire-plugin:2.12.4:test (default-test) @ pxf-delta-connector ---
[INFO] No tests to run.
[INFO] 
[INFO] --- maven-jar-plugin:3.2.0:jar (default-jar) @ pxf-delta-connector ---
[INFO] Building jar: /home/gpadmin/pxf/pxf-delta-connector/target/pxf-delta-connector-1.0-SNAPSHOT.jar
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time:  1.384 s
[INFO] Finished at: 2024-12-29T13:39:49Z
[INFO] ------------------------------------------------------------------------
gpadmin@gpmaster:~/pxf/pxf-delta-connector$ 

5 - To deploy the jar. The script will stop PXF, copy the package, remove all logs and start again PXF
gpadmin@gpmaster:~/pxf/pxf-delta-connector$ ./deploy.sh 
Stopping PXF on coordinator host and 0 segment hosts...
PXF stopped successfully on 1 out of 1 host
Starting PXF on coordinator host and 0 segment hosts...
PXF started successfully on 1 out of 1 host

6 - Connect to Greenplum and create an external Table 

warehouse=# drop external table ext_transactions_d;
DROP FOREIGN TABLE
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
CREATE EXTERNAL TABLE

warehouse=# select * from ext_transactions_d limit 10; 
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
(10 rows)


