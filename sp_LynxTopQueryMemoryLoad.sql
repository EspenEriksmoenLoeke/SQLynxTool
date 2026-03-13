/*
SQLynx Toolkit
Procedure : sp_LynxTopQueryMemoryLoad
Author    : Espen Eriksmoen Løke (SQLynx)

Purpose
-------
Identify queries that contribute the highest memory grant pressure during
a sampling window.
 
This procedure is designed to remain useful even in environments with severe
plan cache bloat by sampling active memory grants and active requests instead
of relying primarily on plan cache metadata.

Recommended installation location
---------------------------------
master database

Installing this procedure in master allows it to be executed from any
database using:

    EXEC sp_LynxTopQueryMemoryLoad

Required permissions
--------------------
VIEW SERVER STATE

Example:

    GRANT VIEW SERVER STATE TO [login];

Notes
-----
1. This procedure samples active memory grants.
2. Results represent observations during the sampling window.
3. Statement text and plans are captured from active requests when available.
*/


CREATE OR ALTER PROCEDURE dbo.sp_LynxTopQueryMemoryLoad
(
    @Top               INT = 50,
    @SampleTimeSeconds INT = 20,
    @SampleIntervalMs  INT = 100
)
AS
BEGIN

    SET NOCOUNT ON;

    -------------------------------------------------------------------------
    -- Defensive parameter handling
    -------------------------------------------------------------------------

    IF @Top IS NULL OR @Top < 1
        SET @Top = 50;

    IF @SampleTimeSeconds IS NULL OR @SampleTimeSeconds < 1
        SET @SampleTimeSeconds = 30;

    IF @SampleIntervalMs IS NULL OR @SampleIntervalMs < 100
        SET @SampleIntervalMs = 1000;

    -------------------------------------------------------------------------
    -- Temp table storing sampled grants
    -------------------------------------------------------------------------

    CREATE TABLE #QueryExecutionGrantedMemory
    (
        sample_time            datetime2(7)  NOT NULL,
        session_id             int           NOT NULL,
        request_id             int           NOT NULL,
        database_id            smallint      NULL,
        used_memory_kb         bigint        NULL,
        granted_memory_kb      bigint        NULL,
        required_memory_kb     bigint        NULL,
        ideal_memory_kb        bigint        NULL,
        query_hash             binary(8)     NULL,
        query_plan_hash        binary(8)     NULL,
        sql_handle             varbinary(64) NULL,
        plan_handle            varbinary(64) NULL,
        statement_start_offset int           NULL,
        statement_end_offset   int           NULL
    );

    CREATE CLUSTERED INDEX IX_QEGM
        ON #QueryExecutionGrantedMemory (sample_time, session_id, request_id);

    -------------------------------------------------------------------------
    -- Sampling loop configuration
    -------------------------------------------------------------------------

    DECLARE
        @EndTime datetime2(7) = DATEADD(SECOND, @SampleTimeSeconds, SYSUTCDATETIME()),
        @Delay   varchar(16);

    SET @Delay =
        '00:00:' +
        RIGHT('00' + CAST(@SampleIntervalMs / 1000 AS varchar(2)),2) +
        '.' +
        RIGHT('000' + CAST(@SampleIntervalMs % 1000 AS varchar(3)),3);

    -------------------------------------------------------------------------
    -- Sampling loop
    -------------------------------------------------------------------------

    WHILE SYSUTCDATETIME() < @EndTime
    BEGIN

        INSERT #QueryExecutionGrantedMemory
        (
            sample_time,
            session_id,
            request_id,
            database_id,
            used_memory_kb,
            granted_memory_kb,
            required_memory_kb,
            ideal_memory_kb,
            query_hash,
            query_plan_hash,
            sql_handle,
            plan_handle,
            statement_start_offset,
            statement_end_offset
        )
        SELECT
            SYSUTCDATETIME(),
            mg.session_id,
            mg.request_id,
            r.database_id,
            mg.used_memory_kb,
            mg.granted_memory_kb,
            mg.required_memory_kb,
            mg.ideal_memory_kb,
            mg.query_hash,
            mg.query_plan_hash,
            r.sql_handle,
            r.plan_handle,
            r.statement_start_offset,
            r.statement_end_offset
        FROM sys.dm_exec_query_memory_grants AS mg
        LEFT JOIN sys.dm_exec_requests AS r
            ON r.session_id = mg.session_id
           AND r.request_id = mg.request_id
        WHERE mg.query_hash IS NOT NULL
          AND mg.query_plan_hash IS NOT NULL;

        WAITFOR DELAY @Delay;

    END;

    DECLARE @TotalSampleMoments bigint;

    SELECT
        @TotalSampleMoments = COUNT(DISTINCT sample_time)
    FROM #QueryExecutionGrantedMemory;

    -------------------------------------------------------------------------
    -- Queries causing highest total memory load
    -------------------------------------------------------------------------

    ;WITH MemoryLoad AS
    (
        SELECT
            query_hash,
            query_plan_hash,
            COUNT(*) AS SampleCount,
            COUNT(DISTINCT sample_time) AS DistinctSampleMoments,
            SUM(granted_memory_kb) / 1024.0 AS MemoryLoadScoreMB,
            SUM(used_memory_kb) / 1024.0 AS UsedMemoryLoadScoreMB,
            AVG(granted_memory_kb) / 1024.0 AS AvgGrantedMemoryMB,
            MAX(granted_memory_kb) / 1024.0 AS MaxGrantedMemoryMB,
            AVG(used_memory_kb) / 1024.0 AS AvgUsedMemoryMB,
            MAX(used_memory_kb) / 1024.0 AS MaxUsedMemoryMB
        FROM #QueryExecutionGrantedMemory
        GROUP BY
            query_hash,
            query_plan_hash
    ),
    BestSample AS
    (
        SELECT
            *,
            ROW_NUMBER() OVER
            (
                PARTITION BY query_hash, query_plan_hash
                ORDER BY granted_memory_kb DESC,
                         used_memory_kb DESC,
                         sample_time DESC
            ) AS rn
        FROM #QueryExecutionGrantedMemory
        WHERE sql_handle IS NOT NULL
    )

    SELECT TOP (@Top)

        MetricName = 'Top queries by memory load',

        SampleWindowSeconds  = @SampleTimeSeconds,
        SampleIntervalMs     = @SampleIntervalMs,
        TotalSampleMoments   = @TotalSampleMoments,

        ml.query_hash,
        ml.query_plan_hash,

        ml.MemoryLoadScoreMB,
        MemoryLoadScoreMBSeconds = ml.MemoryLoadScoreMB * (@SampleIntervalMs / 1000.0),

        ml.UsedMemoryLoadScoreMB,
        UsedMemoryLoadScoreMBSeconds = ml.UsedMemoryLoadScoreMB * (@SampleIntervalMs / 1000.0),

        ml.AvgGrantedMemoryMB,
        ml.MaxGrantedMemoryMB,
        ml.AvgUsedMemoryMB,
        ml.MaxUsedMemoryMB,

        ml.SampleCount,
        ml.DistinctSampleMoments,

        database_name = DB_NAME(COALESCE(bs.database_id, t.dbid)),

        object_name =
            CASE
                WHEN t.objectid IS NOT NULL
                THEN
                    QUOTENAME(OBJECT_SCHEMA_NAME(t.objectid, COALESCE(bs.database_id, t.dbid)))
                    + '.'
                    + QUOTENAME(OBJECT_NAME(t.objectid, COALESCE(bs.database_id, t.dbid)))
            END,

        statement_text = st.statement_text,

        batch_text = t.text,

        query_plan = qp.query_plan

    FROM MemoryLoad ml

    LEFT JOIN BestSample bs
        ON bs.query_hash = ml.query_hash
       AND bs.query_plan_hash = ml.query_plan_hash
       AND bs.rn = 1

    OUTER APPLY sys.dm_exec_sql_text(bs.sql_handle) t

    OUTER APPLY
    (
        SELECT
            statement_text =
                CASE
                    WHEN t.text IS NOT NULL
                     AND bs.statement_start_offset IS NOT NULL
                     AND bs.statement_end_offset IS NOT NULL
                    THEN SUBSTRING
                    (
                        t.text,
                        (bs.statement_start_offset / 2) + 1,
                        (
                            (
                                CASE bs.statement_end_offset
                                    WHEN -1 THEN DATALENGTH(t.text)
                                    ELSE bs.statement_end_offset
                                END
                                - bs.statement_start_offset
                            ) / 2
                        ) + 1
                    )
                    ELSE t.text
                END
    ) st

    OUTER APPLY sys.dm_exec_text_query_plan
    (
        bs.plan_handle,
        bs.statement_start_offset,
        bs.statement_end_offset
    ) qp

    ORDER BY
        ml.MemoryLoadScoreMB DESC,
        ml.MaxGrantedMemoryMB DESC;

END
GO