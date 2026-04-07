CREATE OR ALTER PROCEDURE dbo.sp_LynxTopQueriesGrantedMemoryLoad
(
    @Top               INT = 10,
    @SampleTimeSeconds INT = 30,
    @SampleIntervalMs  INT = 100
)
AS
BEGIN
    SET NOCOUNT ON;

    /************************************************************************************************************
    Author:        Espen Eriksmoen Løke
    Company:       SQLynx AS (Norway)
    Website:       https://sqlynx.no
    Toolkit:       https://sqlynx.no/sqlynx-performance-kit/

    Procedure:     dbo.sp_LynxTopQueriesGrantedMemoryLoad
    Version:       1.0.0
    Release date:  2026-03-22

    Purpose:
        Samples sys.dm_exec_query_memory_grants over a configurable time window and identifies
        the top queries based on:

            1. Total granted memory load (MB over time)
            2. Total unused memory (wasted grant)
            3. Maximum single memory grant

        Designed for SQL Server performance tuning, specifically memory grant analysis.

    Usage:
        EXEC dbo.sp_LynxTopQueriesGrantedMemoryLoad
            @Top = 10,
            @SampleTimeSeconds = 30,
            @SampleIntervalMs = 100;

    Output:
        Returns 3 result sets:

            1. Top queries by granted memory load
            2. Top queries by unused memory load
            3. Top queries by max granted memory

        Each result includes:
            - Query hash / plan hash
            - Aggregated memory metrics
            - Sample counts
            - Database and object name
            - Statement text and batch text
            - Query plan (XML), when available

    Notes:
        - Tuning-focused: only rows with query_hash and query_plan_hash are ranked.
        - Sampling-based approach: results reflect observed activity during the sample window.
        - Very short-lived queries may be underrepresented.
        - Memory grants without an active request row are sampled, but not included in the ranked output.
        - #BestSample is selected to maximize the chance of returning an actual query plan.

    Requirements:
        Requires VIEW SERVER STATE permission.

            GRANT VIEW SERVER STATE TO [login];

        Needed for:
            - sys.dm_exec_query_memory_grants
            - sys.dm_exec_requests
            - sys.dm_exec_sql_text
            - sys.dm_exec_text_query_plan

    Disclaimer:
        Provided as-is by SQLynx AS. Use at your own risk. Test before production use.

    ************************************************************************************************************/

    ---------------------------------------------------------------------
    -- Defensive parameter handling
    ---------------------------------------------------------------------

    IF @Top < 1 SET @Top = 10;
    IF @SampleTimeSeconds < 1 SET @SampleTimeSeconds = 30;
    IF @SampleIntervalMs < 50 SET @SampleIntervalMs = 50;

    ---------------------------------------------------------------------
    -- Temp table for raw samples
    ---------------------------------------------------------------------
    CREATE TABLE #QueryExecutionGrantedMemory
    (
        sample_time            DATETIME2(7)  NOT NULL,
        database_id            SMALLINT      NULL,
        query_hash             BINARY(8)     NULL,
        query_plan_hash        BINARY(8)     NULL,
        sql_handle             VARBINARY(64) NULL,
        plan_handle            VARBINARY(64) NULL,
        statement_start_offset INT           NULL,
        statement_end_offset   INT           NULL,
        granted_memory_kb      BIGINT        NOT NULL,
        used_memory_kb         BIGINT        NULL,
        unused_memory_kb       BIGINT        NOT NULL
    );

    CREATE CLUSTERED INDEX IX_QEGM_SampleTime
        ON #QueryExecutionGrantedMemory(sample_time);

    CREATE INDEX IX_QEGM_Query
        ON #QueryExecutionGrantedMemory(query_hash, query_plan_hash);

    ---------------------------------------------------------------------
    -- Sampling configuration
    ---------------------------------------------------------------------
    DECLARE
        @EndTime DATETIME2(7) = DATEADD(SECOND, @SampleTimeSeconds, SYSUTCDATETIME()),
        @Delay   VARCHAR(16);

    SET @Delay =
        '00:00:' +
        RIGHT('00' + CAST(@SampleIntervalMs / 1000 AS VARCHAR(2)), 2) +
        '.' +
        RIGHT('000' + CAST(@SampleIntervalMs % 1000 AS VARCHAR(3)), 3);

    ---------------------------------------------------------------------
    -- Sampling loop
    ---------------------------------------------------------------------
    WHILE SYSUTCDATETIME() < @EndTime
    BEGIN
        INSERT #QueryExecutionGrantedMemory
        (
            sample_time,
            database_id,
            query_hash,
            query_plan_hash,
            sql_handle,
            plan_handle,
            statement_start_offset,
            statement_end_offset,
            granted_memory_kb,
            used_memory_kb,
            unused_memory_kb
        )
        SELECT
            SYSUTCDATETIME(),
            r.database_id,
            r.query_hash,
            r.query_plan_hash,
            COALESCE(r.sql_handle, mg.sql_handle),
            COALESCE(r.plan_handle, mg.plan_handle),
            r.statement_start_offset,
            r.statement_end_offset,
            mg.granted_memory_kb,
            mg.used_memory_kb,
            CASE
                WHEN mg.granted_memory_kb > COALESCE(mg.used_memory_kb, 0)
                    THEN mg.granted_memory_kb - COALESCE(mg.used_memory_kb, 0)
                ELSE 
                    0
            END
        FROM 
            sys.dm_exec_query_memory_grants AS mg
        LEFT JOIN 
            sys.dm_exec_requests AS r ON (r.session_id = mg.session_id AND r.request_id = mg.request_id)
        WHERE 
            mg.granted_memory_kb > 0
        AND 
            mg.grant_time IS NOT NULL;

        WAITFOR DELAY @Delay;
    END;

    ---------------------------------------------------------------------
    -- Aggregated metrics for tunable queries only
    ---------------------------------------------------------------------
    SELECT
        q.query_hash,
        q.query_plan_hash,

        COUNT(*) AS SampleCount,
        COUNT(DISTINCT q.sample_time) AS DistinctSampleMoments,

        SUM(q.granted_memory_kb) / 1024.0 AS GrantedMemoryLoadScoreMB,
        AVG(q.granted_memory_kb) / 1024.0 AS AvgGrantedMemoryMB,
        MAX(q.granted_memory_kb) / 1024.0 AS MaxGrantedMemoryMB,

        SUM(COALESCE(q.used_memory_kb, 0)) / 1024.0 AS UsedMemoryLoadScoreMB,
        AVG(COALESCE(q.used_memory_kb, 0)) / 1024.0 AS AvgUsedMemoryMB,
        MAX(COALESCE(q.used_memory_kb, 0)) / 1024.0 AS MaxUsedMemoryMB,

        SUM(q.unused_memory_kb) / 1024.0 AS UnusedMemoryLoadScoreMB,
        AVG(q.unused_memory_kb) / 1024.0 AS AvgUnusedMemoryMB,
        MAX(q.unused_memory_kb) / 1024.0 AS MaxUnusedMemoryMB
    INTO 
        #MemoryLoad
    FROM 
        #QueryExecutionGrantedMemory AS q
    WHERE 
        q.query_hash IS NOT NULL
    AND 
        q.query_plan_hash IS NOT NULL
    GROUP BY
        q.query_hash,
        q.query_plan_hash;

    CREATE CLUSTERED INDEX IX_MemoryLoad
        ON #MemoryLoad(query_hash, query_plan_hash);

    ---------------------------------------------------------------------
    -- Best sample per query
    -- Prioritizes rows where an actual query plan can be retrieved
    ---------------------------------------------------------------------
    SELECT
        d.sample_time,
        d.database_id,
        d.query_hash,
        d.query_plan_hash,
        d.sql_handle,
        d.plan_handle,
        d.statement_start_offset,
        d.statement_end_offset,
        d.granted_memory_kb,
        d.used_memory_kb,
        d.unused_memory_kb
    INTO 
        #BestSample
    FROM
    (
        SELECT
            q.sample_time,
            q.database_id,
            q.query_hash,
            q.query_plan_hash,
            q.sql_handle,
            q.plan_handle,
            q.statement_start_offset,
            q.statement_end_offset,
            q.granted_memory_kb,
            q.used_memory_kb,
            q.unused_memory_kb,
            ROW_NUMBER() OVER
            (
                PARTITION BY q.query_hash, q.query_plan_hash
                ORDER BY
                    CASE WHEN qp.query_plan IS NOT NULL THEN 0 ELSE 1 END ASC,
                    CASE WHEN q.plan_handle IS NOT NULL THEN 0 ELSE 1 END ASC,
                    CASE WHEN q.sql_handle IS NOT NULL THEN 0 ELSE 1 END ASC,
                    q.sample_time DESC,
                    q.granted_memory_kb DESC,
                    COALESCE(q.used_memory_kb, 0) DESC                    
            ) AS rn
        FROM 
            #QueryExecutionGrantedMemory AS q
        OUTER APPLY 
            sys.dm_exec_text_query_plan
            (
                q.plan_handle,
                q.statement_start_offset,
                q.statement_end_offset
            ) AS qp
        WHERE 
            q.query_hash IS NOT NULL
        AND 
            q.query_plan_hash IS NOT NULL
        AND 
            (q.sql_handle IS NOT NULL OR q.plan_handle IS NOT NULL)
    ) AS d
    WHERE 
        d.rn = 1;

    CREATE CLUSTERED INDEX IX_BestSample
        ON #BestSample(query_hash, query_plan_hash);

    ---------------------------------------------------------------------
    -- Final reusable result set
    ---------------------------------------------------------------------

    SELECT
        ml.query_hash,
        ml.query_plan_hash,

        ml.GrantedMemoryLoadScoreMB,
        ml.UsedMemoryLoadScoreMB,
        ml.UnusedMemoryLoadScoreMB,

        ml.AvgGrantedMemoryMB,
        ml.MaxGrantedMemoryMB,

        ml.AvgUsedMemoryMB,
        ml.MaxUsedMemoryMB,

        ml.AvgUnusedMemoryMB,
        ml.MaxUnusedMemoryMB,

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

        statement_text =
            CASE
                WHEN t.text IS NOT NULL
                 AND bs.statement_start_offset IS NOT NULL
                THEN
                    SUBSTRING
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
            END,

        batch_text = t.text,
        query_plan = CAST(qp.query_plan AS XML)
    INTO 
        #Final
    FROM 
        #MemoryLoad AS ml
    LEFT JOIN 
        #BestSample AS bs ON (bs.query_hash = ml.query_hash AND bs.query_plan_hash = ml.query_plan_hash)
    OUTER APPLY 
        sys.dm_exec_sql_text(bs.sql_handle) AS t
    OUTER APPLY 
        sys.dm_exec_text_query_plan
        (
            bs.plan_handle,
            bs.statement_start_offset,
            bs.statement_end_offset
        ) AS qp;

    ---------------------------------------------------------------------
    -- Result set 1: Top queries by granted memory load
    ---------------------------------------------------------------------

    SELECT TOP (@Top)
        ResultSetName = 'Top queries by granted memory load',        
        *
    FROM 
        #Final
    ORDER BY
        GrantedMemoryLoadScoreMB DESC,
        UsedMemoryLoadScoreMB ASC,
        MaxGrantedMemoryMB DESC
        

    ---------------------------------------------------------------------
    -- Result set 2: Top queries by unused memory load
    ---------------------------------------------------------------------
    SELECT TOP (@Top)
        ResultSetName = 'Top queries by unused memory load',
        *
    FROM 
        #Final
    ORDER BY
        UnusedMemoryLoadScoreMB DESC,
        GrantedMemoryLoadScoreMB DESC,
        MaxGrantedMemoryMB DESC
        
    ---------------------------------------------------------------------
    -- Result set 3: Top queries by max granted memory
    ---------------------------------------------------------------------
    SELECT TOP (@Top)
        ResultSetName = 'Top queries by max granted memory',
        *
    FROM 
        #Final
    ORDER BY
        MaxGrantedMemoryMB DESC,
        GrantedMemoryLoadScoreMB DESC,
        UnusedMemoryLoadScoreMB DESC
    OPTION (RECOMPILE);
END
GO
