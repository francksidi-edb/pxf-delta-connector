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



