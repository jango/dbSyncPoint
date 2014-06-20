using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using System.Threading;
using System.Data;
using DBSync;


namespace POC
{
    class Program
    {
        private static string CONN_STR = @"Server=localhost\SQLExpress;Database=PERF_LOCK;Integrated Security=True;Max Pool Size=10000";

        static void Main(string[] args)
        {
            Random rnd = new Random();

            // ID of the run.
            String runID = DateTime.Now.ToString("yyyy-MM-dd-HH-mm-ss-ffff");

            // A list to keep track of the threads threads.
            List<Thread> threads = new List<Thread>();


            // Start client threads.
            for (int i = 0; i < 100; i++)
            {
                DBSync.VirtualUser vu = new DBSync.VirtualUser(CONN_STR, i.ToString(), runID, rnd);
                Thread t = new Thread((new User(vu)).run);
                threads.Add(t);
                t.Start();
            }

            // Make sure threads complete.
            foreach (Thread t in threads)
            {
                t.Join();
            }

            // Pause.
            Console.WriteLine("Completed. Paused until a key is pressed.");
            Console.ReadLine();

        }

        class User
        {
            private DBSync.VirtualUser vu = null;

            public User(DBSync.VirtualUser vu)
            {
                this.vu = vu;
            }

            public void run()
            {
                Console.WriteLine("Syncing for the POINT_1 at point {0}", DateTime.Now.ToString("yyyy-MM-dd-HH-mm-ss-ffff"));
                vu.WaitForSync("POINT_1");
                Console.WriteLine("Got to POINT_1 at {0}", DateTime.Now.ToString("yyyy-MM-dd-HH-mm-ss-ffff"));

                Console.WriteLine("Syncing for the POINT_2 at point {0}", DateTime.Now.ToString("yyyy-MM-dd-HH-mm-ss-ffff"));
                vu.WaitForSync("POINT_2");
                Console.WriteLine("Got to POINT_2 at {0}", DateTime.Now.ToString("yyyy-MM-dd-HH-mm-ss-ffff"));
            }
        }
    }
}
