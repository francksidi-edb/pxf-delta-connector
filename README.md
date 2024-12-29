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
