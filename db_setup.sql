USE PERF_LOCK;

-- Enables notification on the database.
ALTER DATABASE PERF_LOCK SET ENABLE_BROKER;

-- Keeps track of lock requests.
CREATE TABLE dbo.LOCK_REQUEST(
    ID INTEGER IDENTITY PRIMARY KEY,

    -- ID of the run.
    RUN_ID VARCHAR(50),

    -- Unique ID of the Virtual User.
    USER_ID VARCHAR(50),

    -- Unique ID of the Lock.
    LOCK_ID VARCHAR(50),

    -- When the lock was requested (when this record was created).
    REQUEST_DT DATETIME,

    UNIQUE(RUN_ID, USER_ID, LOCK_ID)
);
GO

-- Keeps track of when a given RUN_ID, LOCK_ID lock
-- can be released because all of Virtual Users have it
-- requested.
CREATE TABLE dbo.LOCK_RELEASE(
    ID INT IDENTITY PRIMARY KEY,

    -- ID of the run.
    RUN_ID VARCHAR(50),

    -- Unique ID of the Lock.
    LOCK_ID VARCHAR(50),

    -- When the lock was released (when this record was created).
    RELEASE_DT DATETIME,

    -- When the Virtual User should execute the action.
    ACTION_DT DATETIME,

    UNIQUE(RUN_ID, LOCK_ID)
);
GO

-- Trigger that will create a new record in LOCK_RELEASE when
-- all Virtual Users have requested the lock.
CREATE TRIGGER dbo.TRG_LOCK_REQUEST_RELEASE ON dbo.LOCK_REQUEST
FOR INSERT
AS
    -- How soon (seconds) the action should happen after
    -- Virtual Users have been notified about lock release.
    DECLARE @TIME_DELTA INT = 30;

    -- Total number of users for each synchronization point
    -- before the lock is released.
    DECLARE @REQUESTS_EXPECTED INT = 100;

    -- Variable to store how many requests we have currently.
    DECLARE @REQUESTS_ACTUAL INT = 0;

        -- Calculate how many users have requested the same
        -- lock as the latest inserted record.
        SET @REQUESTS_ACTUAL = (
                                    SELECT
                                       COUNT(*)
                                    FROM
                                       -- READPAST is important to avoid locking.
                                       dbo.LOCK_REQUEST LR WITH(READPAST)
                                    WHERE
                                       LR.LOCK_ID = (SELECT LOCK_ID FROM INSERTED)
                                       AND LR.RUN_ID = (SELECT RUN_ID FROM INSERTED)
                                )

        -- Check if we need to release the lock.
        IF (@REQUESTS_EXPECTED = @REQUESTS_ACTUAL) 
         BEGIN
            INSERT INTO
                dbo.LOCK_RELEASE(RUN_ID, LOCK_ID, RELEASE_DT, ACTION_DT)
            VALUES((SELECT RUN_ID FROM INSERTED),
                   (SELECT LOCK_ID FROM INSERTED),
                    CURRENT_TIMESTAMP,
                    DATEADD(ss, @TIME_DELTA, CURRENT_TIMESTAMP));
         END
GO

-- Stored procedure for the Virtual User to request a lock.
CREATE PROCEDURE dbo.sp_REQUEST_LOCK @RUN_ID VARCHAR(50),
                                  @USER_ID VARCHAR(50),
                                  @LOCK_ID VARCHAR(50)
AS
    INSERT INTO
        dbo.LOCK_REQUEST(RUN_ID, USER_ID, LOCK_ID, REQUEST_DT)
    VALUES (@RUN_ID, @USER_ID, @LOCK_ID, CURRENT_TIMESTAMP) 
GO