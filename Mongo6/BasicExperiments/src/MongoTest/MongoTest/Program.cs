// See https://aka.ms/new-console-template for more information
using MongoDB.Bson;
using MongoDB.Driver;
using System.Diagnostics;

Console.WriteLine("Hello, Mongo!");

MongoUrlBuilder builder = new MongoUrlBuilder();
builder.Username = "admin";
builder.Password = "12345";
builder.AuthenticationSource = "admin";
builder.DatabaseName = "csharp";

var url = builder.ToMongoUrl();
Console.WriteLine("Url is {0}", url);
//MongoUrl url = new MongoUrl("mongodb://admin:12345@localhost/csharp?authSource=admin");
MongoClient client = new MongoClient(url);
var db = client.GetDatabase(url.DatabaseName);

await client.DropDatabaseAsync(db.DatabaseNamespace.DatabaseName);

var collection = db.GetCollection<BsonDocument>("documents");

Stopwatch sw = Stopwatch.StartNew();
var documents = new List<BsonDocument>(1000);
for (int i = 0; i < 1000000; i++)
{
    var doc = new BsonDocument();
    doc["_id"] = $"Document_{i}";
    doc["count"] = i * 10;
    doc["title"] = $"Titolo documento {i}";
    doc["text"] = new string('x', i % 10000);
    //if (45 == i)
    //{
    //    doc["_id"] = $"Document_1";
    //}

    documents.Add(doc);

    if (documents.Count == 10) 
    {
        try
        {
            await collection.InsertManyAsync(
                documents,
                new InsertManyOptions()
                {
                    IsOrdered = false,
                });

            Console.WriteLine("Inserito batch {0} {1}", i, sw.ElapsedMilliseconds);
            documents.Clear();
        }
        catch (MongoBulkWriteException<BsonDocument> bex)
        {
            var failedDocuments = bex.WriteErrors.Select(i => documents[i.Index]).ToList();
            //Logga da qualche parte i documenti falliti
            documents.Clear();
            Console.Write(bex);
        }
        catch (Exception ex)
        {
            Console.Write(ex);
            //GESTISCI L'error
            Environment.Exit(1);
        }

        
    }
}

//await collection.InsertOneAsync(doc);





//await collection.InsertManyAsync(
//    documents,
//    new InsertManyOptions()
//    {
//        IsOrdered = false,
//    });

//foreach (var doc in documents)
//{
//    await collection.InsertOneAsync(doc);
//}

sw.Stop();
Console.WriteLine("Inserted element took: {0}ms", sw.ElapsedMilliseconds);


Console.ReadKey();

