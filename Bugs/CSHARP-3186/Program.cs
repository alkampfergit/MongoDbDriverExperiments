using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;

namespace mongotest
{
    internal class Program
    {
        public class Event
        {
            [JsonConstructor]
            private Event()
            {
            }

            public Event(
                Container newContainer,
                Container oldContainer
            )
            {
                Id = Guid.NewGuid().ToString();
                NewLayout = newContainer ?? throw new ArgumentNullException(nameof(newContainer));
                OldLayout = oldContainer;
            }

            public string Id { get; private set; }

            public Container NewLayout { get; private set; }

            public Container OldLayout { get; private set; }
        }

        public class Container
        {
            public Container(List<BaseTest> tests)
            {
                Tests = tests;
            }

            public List<BaseTest> Tests { get; private set; }
        }

        public abstract class BaseTest
        {
            protected BaseTest(string name, string surname)
            {
                Name = name;
                Surname = surname;
            }

            public string Name { get; protected set; }

            public string Surname { get; protected set; }
        }

        public class Derived : BaseTest
        {
            public Derived(string name, string surname, string address, string otherString) : base(name, surname)
            {
                Address = address;
                OtherString = otherString;
            }

            public string Address { get; private set; }
            
            /// <summary>
            /// Important: if this property is made public, everythign works as expected.
            /// </summary>
            public string OtherString { get; private set; }
        }

        public class Derived2 : BaseTest
        {
            public Derived2(string name, string surname, string address) : base(name, surname)
            {
                Address = address;
            }

            public string Address { get; private set; }
        }

        public class Loop : BaseTest
        {
            public Loop(string name, string surname, List<BaseTest> tests) : base(name, surname)
            {
                Tests = tests;
            }

            public List<BaseTest> Tests { get; private set; }
        }

        private static void Main(string[] args)
        {
            //If you replace the default convention pack using old version of immutabl
            //class convention everything works.
            //ConventionRegistryHelper.ReplaceDefaultConventionPack();
            
            var mongoUrl = new MongoUrl("mongodb://admin:password@localhost/test-database?authSource=admin");
            var client = new MongoClient(mongoUrl);
            var db = client.GetDatabase(mongoUrl.DatabaseName);

            var id = SaveData(db);

            BsonClassMap.LookupClassMap(typeof(Derived));
            BsonClassMap.LookupClassMap(typeof(Derived2));
            BsonClassMap.LookupClassMap(typeof(Loop));

            var coll = db.GetCollection<Event>("BaseTest");
            var loaded = coll.AsQueryable().Single(c => c.Id == id);
            Console.WriteLine($"Loaded correctly {loaded.Id}, press a key to continue");
            Console.ReadKey();
        }

        private static string SaveData(IMongoDatabase db)
        {
            db.DropCollection("BaseTest");
            var coll = db.GetCollection<Event>("BaseTest");

            var list = new List<BaseTest>();
            Derived obj = new Derived("gian maria", "Verdi", "Via le mani dal culo", "other String");
            list.Add(obj);

            var container = new Container(list);

            var evt = new Event(container, null);
            coll.InsertOne(evt);

            var loaded = coll.AsQueryable().Single(c => c.Id == evt.Id);
            Console.WriteLine("Loaded " + loaded);

            //This is the most important part of the test, we are removing a property
            //from the serialized object. Until 2.7 driver this is not a problem, it simulates
            //the situation where we have an object serialized without property OtherString
            //then we add property OtherString to the object supposing that, for existing
            //Serialized object without that property, it simply value null.
            coll.FindOneAndUpdate(
                Builders<Event>.Filter.Eq("_id", evt.Id),
                Builders<Event>.Update.Unset("NewLayout.Tests.0.OtherString"));

            return evt.Id;
        }
    }
}
