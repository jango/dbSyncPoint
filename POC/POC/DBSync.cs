using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Data.SqlClient;
using System.Data;
using System.Diagnostics;

namespace DBSync
{
        class VirtualUser
        {
            // SQL used to retrive lock release record from the database.
            private static string SQL_GET_LOCK_RELEASE_RECORD = "SELECT ACTION_DT FROM dbo.LOCK_RELEASE WHERE RUN_ID = @RUN_ID AND LOCK_ID = @LOCK_ID";
            
            // Name of the column in which action DT is recorded.
            private static string SQL_ACTION_DT_COLUMN_NAME = "ACTION_DT";

            // Number of retries for requesting the lock (the more users the bigger chance of timeouts or deadlocks).
            private static int MAX_LOCK_REQUEST_RETRIES = 10000;

            // Unique identifier (50 chars) of this user ID.
            private string userID = null;

            // Unique identifier (50 chars) of the run this user is part of.
            private string runID = null;

            // Unique identifier (50 chars) of the run.
            private string lockID = null;

            // DB connection String, trusted connection is assumed.
            private string connStr = null;

            // Random number generator to be used to break ties between Virtual Users.
            private Random rnd = null;

            // Indicates whether last notification of lock release has been processed.
            private bool lastLockNotificationProcessed = false;

            /// <summary>Parses database timestamp into DateTime object.</summary>
            private DateTime parseActionDateTime(string dt)
            {
                return DateTime.Parse(dt);
            }


            /// <summary>Lock release notification handler. Essentialy,
            /// it will check when the action needs to happen and then
            /// wait in a loop until it's time to perform the action, at
            /// which point the method will return.</summary>
            private void OnLockReleased(object sender, SqlNotificationEventArgs e)
            {
                DateTime startTime = DateTime.MinValue;

                Trace.TraceInformation("User {0} notification about lock '{1}' received.", userID, lockID);

                using (SqlConnection con = new SqlConnection(this.connStr))
                {
                    using (SqlCommand notificationCmd = new SqlCommand(SQL_GET_LOCK_RELEASE_RECORD, con))
                    {
                        notificationCmd.CommandType = CommandType.Text;
                        notificationCmd.Parameters.Add("@RUN_ID", SqlDbType.VarChar).Value = this.runID;
                        notificationCmd.Parameters.Add("@LOCK_ID", SqlDbType.VarChar).Value = this.lockID;

                        con.Open();

                        using (SqlDataReader reader = notificationCmd.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                startTime = parseActionDateTime(reader[SQL_ACTION_DT_COLUMN_NAME].ToString());
                                break;
                            }
                        }
                    }
                }

                Trace.TraceInformation("User {0}: it's {1} halting till {2}", this.userID, DateTime.Now.ToString(), startTime.ToString());

                // Sleep for one second repeatedly, until the target time is reached.
                while (startTime > DateTime.Now)
                {
                    Thread.Sleep(TimeSpan.FromSeconds(1));
                }


                lastLockNotificationProcessed = true;
            }

            public VirtualUser(string connStr, string userID, string runID, Random rnd)
            {
                this.userID = userID;
                this.runID = runID;
                this.connStr = connStr;
                this.rnd = rnd;
            }

            public void WaitForSync(string lockID)
            {
                this.lockID = lockID;
                SqlConnection conn = new SqlConnection(this.connStr);
                conn.Open();

                // Attach a notification listener.
                SqlDependency.Start(this.connStr);
                using (SqlConnection con = new SqlConnection(this.connStr))
                {
                    using (SqlCommand notificationCmd = new SqlCommand(SQL_GET_LOCK_RELEASE_RECORD, con))
                    {
                        SqlDependency dependency = new SqlDependency(notificationCmd);
                        dependency.OnChange += new OnChangeEventHandler(OnLockReleased);

                        notificationCmd.CommandType = CommandType.Text;

                        notificationCmd.Parameters.Add("@RUN_ID", SqlDbType.VarChar).Value = this.runID;
                        notificationCmd.Parameters.Add("@LOCK_ID", SqlDbType.VarChar).Value = this.lockID;

                        con.Open();

                        // Need to do this to attach the listener.
                        using (SqlDataReader reader = notificationCmd.ExecuteReader()){}
                    }
                }

                // Request a lock.
                Trace.TraceInformation("User {0} requsting a lock.", userID);
                lastLockNotificationProcessed = false;
                using (SqlConnection con = new SqlConnection(this.connStr))
                {
                    // Execute stored procedure that insers a record in the lock request table.
                    using (SqlCommand lockRequestCmd = new SqlCommand("dbo.sp_REQUEST_LOCK", con))
                    {
                        lockRequestCmd.CommandType = CommandType.StoredProcedure;

                        lockRequestCmd.Parameters.Add("@RUN_ID", SqlDbType.VarChar).Value = this.runID;
                        lockRequestCmd.Parameters.Add("@USER_ID", SqlDbType.VarChar).Value = this.userID;
                        lockRequestCmd.Parameters.Add("@LOCK_ID", SqlDbType.VarChar).Value = this.lockID;

                        con.Open();


                        int retryCount = MAX_LOCK_REQUEST_RETRIES;

                        // Retry requesting the lock. On failure, sleep for a
                        // few seconds randomely.
                        while (retryCount-- >= 0)
                        {
                            try
                            {
                                lockRequestCmd.ExecuteNonQuery();

                                // That's important, we just need one successful request.
                                break;
                            }
                            catch (SqlException ex)
                            {
                                // Deadlock.
                                if (ex.Number != 1205 && ex.Number != 1204 && ex.Number != -2)
                                {
                                    throw;
                                }

                                if (retryCount <= 0) throw;

                                // Sleep randomely to avoid deadlocks.
                                Thread.Sleep(TimeSpan.FromSeconds(rnd.Next(0, 5)));
                            }
                        }
                    }
                }


                // In case we've reached this point but the lock still has not been released.
                while (!lastLockNotificationProcessed)
                {
                    Thread.Sleep(TimeSpan.FromSeconds(1));
                }
            }
        }
    }
