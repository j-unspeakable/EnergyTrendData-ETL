# Delta Table Optimization Strategy

This document outlines the strategy for structuring a Delta table equivalent of this project to optimize read and write performance. The optimization strategies are based on typical read and write patterns, concurrency management, and handling deduplication and upserts.

## Table of Contents

- [Read Patterns](#read-patterns)
- [Write Patterns](#write-patterns)
- [Concurrency](#concurrency)
- [Deduplication and Upserts](#deduplication-and-upserts)
- [Assumptions About Table Usage](#assumptions-about-table-usage)
- [Proposed Delta Table Schema](#proposed-delta-table-schema)
- [Next Steps](#next-steps)

## Read Patterns

**Typical Read Queries**: The table is likely to be queried for specific time periods (e.g., quarterly data for specific years) and filtered by specific categories (e.g., "Crude oil", "NGLs").

**Optimization Strategy**:
- **Partitioning**: Partition the Delta table by a `year` column extracted from the period columns. This approach helps efficiently read data for specific years thus helping with time-based queries and reducing the data scanned for each query
- **Data Clustering (Z-Ordering)**: Z-ordering is a technique to colocate related information in the same set of files. This co-locality is automatically used by Delta Lake on Databricks data-skipping algorithms. The `quarter` column has low cardinality (only four distinct values), making it a poor candidate for Z-ordering, as it won't significantly benefit from data colocation. Same is the case with the `cateory` column too. 
Consider Z-Ordering on `processed_date` since it likely has high cardinality and can optimize queries that filter by specific dates or date ranges, making it a good candidate for Z-ordering if such queries are common.

## Write Patterns

**Write Frequency**: Assuming that new data is written quarterly, we might expect periodic writes every quarter or monthly. There might also be historical data corrections or upserts.

**Optimization Strategy**:
- **Batch Writes**: Accumulate writes and perform batch updates to minimize small file creation and optimize transaction management.
- **Upsert Handling**: Use `MERGE` operations in Delta Lake to handle upserts efficiently, ensuring only the changed or new data is written.

## Concurrency

**Concurrent Reads and Writes**: The Delta Lake format inherently supports ACID transactions, which helps manage concurrent reads and writes effectively.

**Optimization Strategy**:
- **Isolation Levels**: Utilize Delta Lake's support for multiple isolation levels (e.g., Serializable) to manage concurrency without compromising performance.
- **Versioning**: Leverage Delta Lake's versioning capabilities to ensure readers always have access to a consistent snapshot of the data.

## Deduplication and Upserts

- **Deduplication**: Deduplicate data by defining a unique key or a composite key, such as a combination of category, year, and quarter.
- **Upserts**:
  - **MERGE Statement**: Use Delta Lake's `MERGE` statement to perform upserts. This is particularly useful when integrating new data that may have overlaps or need updates on existing records.
  - **Primary Key Management**: Define primary keys or unique constraints to manage upserts and avoid duplicates effectively.

## Assumptions About Table Usage

- **Data is time-series in nature**: Primarily focused on tracking production or related metrics over time.
- **Frequent Reads with Periodic Writes**: Read operations are likely more frequent than writes, but writes occur periodically (e.g., quarterly).
- **Analytical Workload**: The table will likely support analytical workloads, so optimizing for both read and write performance is crucial.

## Proposed Delta Table Schema

```sql
CREATE TABLE energy_trend (
  category STRING,
  year INT,
  quarter STRING,
  value DOUBLE,
  processed_date TIMESTAMP,
  filename STRING
)
USING DELTA
PARTITIONED BY (year)
LOCATION '/delta/energy_trend';
```

To implement Z-ordering, you would typically run:
```sql
OPTIMIZE energy_trend
ZORDER BY (processed_date);
```

## Next Steps

1. **Implement Partitioning and Clustering**: 
   - Apply the partitioning strategy by `year` and consider Z-ordering by `processed_date` to enhance performance for date-based queries.

2. **Optimize Write Operations**: 
   - Use batch processing for writes and implement `MERGE` operations for efficient upserts, reducing the creation of small files and ensuring data consistency.

3. **Monitor and Tune Performance**: 
   - Continuously monitor read and write patterns to identify performance bottlenecks and adjust the partitioning and Z-ordering strategies accordingly.

4. **Ensure Data Quality and Consistency**: 
   - Regularly validate data to ensure deduplication and correct upserts, maintaining data quality and table integrity.