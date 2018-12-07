using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using MongoDB.Driver;

namespace SomeTestOnMongoDriver
{
    class Program
    {
        static void Main(string[] args)
        {
            //const string connectionString = "mongodb://admin:123456##@localhost/perf-test?authSource=admin";
            const string connectionString = "mongodb://admin:123456##@localhost/perf-test?authSource=admin&maxPoolSize=1000";
            var url = new MongoUrl(connectionString);
            var client = new MongoClient(url);
            var database = client.GetDatabase(url.DatabaseName);
            var collection = database.GetCollection<Document>("values");

            database.DropCollection("values");
            PerformSyncInsertWithSession(client, collection);
            database.DropCollection("values");
            PerformSyncInsertWithoutSession(client, collection);

            database.DropCollection("values");
            PerformAsyncInsertWithSession(client, collection).Wait();
            database.DropCollection("values");
            PerformAsyncInsertWithoutSession(client, collection).Wait();

            Console.WriteLine("Press a Key to continue");
            Console.ReadKey();
        }

        private static void PerformSyncInsertWithSession(MongoClient client, IMongoCollection<Document> collection)
        {
            var sw = Stopwatch.StartNew();
            var tasks = Enumerable
                .Range(1, 1000)
                .Select(value =>
                {
                    return Task.Run(() =>
                    {
                        using (var session = client.StartSession())
                        {
                            for (var i = 0; i < 10; i++)
                                collection.InsertOne(session, new Document());
                        }
                    });
                });
            Task.WaitAll(tasks.ToArray());
            sw.Stop();
            Console.WriteLine("Session Sync Inserted: " + collection.AsQueryable().Count() + " took: " + sw.ElapsedMilliseconds + "ms");
        }

        private static void PerformSyncInsertWithoutSession(MongoClient client, IMongoCollection<Document> collection)
        {
            var sw = Stopwatch.StartNew();
            var tasks = Enumerable
                .Range(1, 1000)
                .Select(value =>
                {
                    return Task.Run(() =>
                    {
                        for (var i = 0; i < 10; i++)
                            collection.InsertOne(new Document());
                    });
                });
            Task.WaitAll(tasks.ToArray());
            sw.Stop();
            Console.WriteLine("Without Session Sync Inserted: " + collection.AsQueryable().Count() + " took: " + sw.ElapsedMilliseconds + "ms");
        }

        private static async Task PerformAsyncInsertWithoutSession(MongoClient client, IMongoCollection<Document> collection)
        {
            var sw = Stopwatch.StartNew();
            var tasks = Enumerable
                .Range(1, 1000)
                .Select(value =>
                {
                    return Task.Run(async () =>
                    {
                        for (var i = 0; i < 10; i++)
                            await collection.InsertOneAsync(new Document()).ConfigureAwait(false);
                    });
                });
            await Task.WhenAll(tasks.ToArray());
            sw.Stop();
            Console.WriteLine("Without Session Async Inserted: " + collection.AsQueryable().Count() + " took: " + sw.ElapsedMilliseconds + "ms");
        }

        private static async Task PerformAsyncInsertWithSession(MongoClient client, IMongoCollection<Document> collection)
        {
            var sw = Stopwatch.StartNew();
            var tasks = Enumerable
                .Range(1, 1000)
                .Select(value =>
                {
                    return Task.Run(async () =>
                    {
                        using (var session = client.StartSession())
                        {
                            for (var i = 0; i < 10; i++)
                                await collection.InsertOneAsync(session, new Document()).ConfigureAwait(false);
                        }
                    });
                });
            await Task.WhenAll(tasks.ToArray());
            sw.Stop();
            Console.WriteLine("Session Async Inserted: " + collection.AsQueryable().Count() + " took: " + sw.ElapsedMilliseconds + "ms");
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
