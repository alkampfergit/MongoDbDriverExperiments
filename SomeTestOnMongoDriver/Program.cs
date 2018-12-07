using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MongoDB.Driver;

namespace SomeTestOnMongoDriver
{
    class Program
    {
        static void Main(string[] args)
        {
            //const string connectionString = "mongodb://admin:123456##@localhost/perf-test?authSource=admin&waitQueueTimeoutMS=10000";
            const string connectionString = "mongodb://admin:123456##@localhost/perf-test?authSource=admin&maxPoolSize=1000&waitQueueTimeoutMS=10000";
            var url = new MongoUrl(connectionString);
            client = new MongoClient(url);
            var database = client.GetDatabase(url.DatabaseName);
            collection = database.GetCollection<Document>("values");

            //database.DropCollection("values");
            //PerformSyncInsertWithSession();
            //database.DropCollection("values");
            //PerformSyncInsertWithoutSession();

            try
            {
                database.DropCollection("values");
                PerformAsyncInsertWithSession().Wait();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[{concurrentJobs}]/[{maxConcurrentJobs}]Cached exception async insert with session {ex.Message}");
            }

            try
            {
                database.DropCollection("values");
                PerformAsyncInsertWithoutSession().Wait();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[{concurrentJobs}]/[{maxConcurrentJobs}]Cached exception async insert without session  {ex.Message}");
            }
            Console.WriteLine("Press a Key to continue");
            Console.ReadKey();
        }

        private static MongoClient client;
        private static IMongoCollection<Document> collection;
        private static Int32 concurrentJobs;
        private static Int32 maxConcurrentJobs;

        private static void PerformSyncInsertWithSession()
        {
            concurrentJobs = maxConcurrentJobs = 0;
            var sw = Stopwatch.StartNew();
            var tasks = Enumerable
                .Range(1, 1000)
                .Select(value =>
                {
                    return Task.Run(() =>
                    {
                        //each time I run a task I increment concurrent job
                        Interlocked.Increment(ref concurrentJobs);
                        maxConcurrentJobs = concurrentJobs > maxConcurrentJobs ? concurrentJobs : maxConcurrentJobs;
                        using (var session = client.StartSession())
                        {
                            for (var i = 0; i < 10; i++)
                                collection.InsertOne(session, new Document());
                        }
                        Interlocked.Decrement(ref concurrentJobs);
                    });
                });
            Task.WaitAll(tasks.ToArray());
            sw.Stop();
            Console.WriteLine("Session Sync Inserted: " + collection.AsQueryable().Count() + " took: " + sw.ElapsedMilliseconds + "ms Max concurrent jobs: " + maxConcurrentJobs);
        }

        private static void PerformSyncInsertWithoutSession()
        {
            concurrentJobs = maxConcurrentJobs = 0;
            var sw = Stopwatch.StartNew();
            var tasks = Enumerable
                .Range(1, 1000)
                .Select(value =>
                {
                    return Task.Run(() =>
                    {
                        //each time I run a task I increment concurrent job
                        Interlocked.Increment(ref concurrentJobs);
                        maxConcurrentJobs = concurrentJobs > maxConcurrentJobs ? concurrentJobs : maxConcurrentJobs;
                        for (var i = 0; i < 10; i++)
                            collection.InsertOne(new Document());
                        Interlocked.Decrement(ref concurrentJobs);
                    });
                });
            Task.WaitAll(tasks.ToArray());
            sw.Stop();
            Console.WriteLine("Without Session Sync Inserted: " + collection.AsQueryable().Count() + " took: " + sw.ElapsedMilliseconds + "ms Max concurrent jobs: " + maxConcurrentJobs);
        }

        private static async Task PerformAsyncInsertWithoutSession()
        {
            concurrentJobs = maxConcurrentJobs = 0;
            var sw = Stopwatch.StartNew();
            var tasks = Enumerable
                .Range(1, 1000)
                .Select(value =>
                {
                    return Task<Int32>.Run(InnerPerformInsertAsyncWithoutSession);
                });
            await Task.WhenAll(tasks.ToArray());
            sw.Stop();
            Console.WriteLine("Without Session Async Inserted: " + collection.AsQueryable().Count() + " took: " + sw.ElapsedMilliseconds + "ms Max concurrent jobs: " + maxConcurrentJobs);
        }

        private static async Task<Int32> InnerPerformInsertAsyncWithoutSession()
        {
            Interlocked.Increment(ref concurrentJobs);
            maxConcurrentJobs = concurrentJobs > maxConcurrentJobs ? concurrentJobs : maxConcurrentJobs;
            for (var i = 0; i < 10; i++)
                await collection.InsertOneAsync(new Document()).ConfigureAwait(false);

            Interlocked.Decrement(ref concurrentJobs);
            return concurrentJobs;
        }

        private static async Task PerformAsyncInsertWithSession()
        {
            concurrentJobs = maxConcurrentJobs = 0;
            var sw = Stopwatch.StartNew();
            var tasks = Enumerable
                .Range(1, 1000)
                .Select(value =>
                {
                    return Task<int>.Run(InnerPerformInsertAsyncWithSession);
                });
            await Task.WhenAll(tasks.ToArray());
            sw.Stop();
            Console.WriteLine("Session Async Inserted: " + collection.AsQueryable().Count() + " took: " + sw.ElapsedMilliseconds + "ms Max concurrent jobs: " + maxConcurrentJobs);
        }

        private static async Task<int> InnerPerformInsertAsyncWithSession()
        {
            //each time I run a task I increment concurrent job
            Interlocked.Increment(ref concurrentJobs);
            maxConcurrentJobs = concurrentJobs > maxConcurrentJobs ? concurrentJobs : maxConcurrentJobs;
            using (var session = client.StartSession())
            {
                for (var i = 0; i < 10; i++)
                    await collection.InsertOneAsync(session, new Document()).ConfigureAwait(false);
            }

            Interlocked.Decrement(ref concurrentJobs);
            return concurrentJobs;
        }
    }

    public class Document
    {
        public Document()
        {
            Id = Guid.NewGuid().ToString();
        }

        public String Id { get; set; }
    }
}
