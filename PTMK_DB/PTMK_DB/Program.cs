using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;

namespace PTMK_DB
{
    internal class Program
    {
        const string connectionString = "mongodb://127.0.0.1:27017";
        const string databaseName = "test_db";
        const string collectionName = "people";

        static Random r = new Random();

        static async Task Main(string[] args)
        {

            var client = new MongoClient(connectionString);
            var db = client.GetDatabase(databaseName);
            var collection = db.GetCollection<PeopleModel>(collectionName);

            int param = int.Parse(args[0]);
            Stopwatch sw = Stopwatch.StartNew();
            switch (param)
            {
                case 1:
                    await CreateExampleDBAsync(collection);
                    break;
                case 2:
                    await AddPersonAsync(collection, args[1], args[2], args[3], args[4], args[5]);
                    break;
                case 3:
                    await SortAsync(collection);
                    break;
                case 4:
                    await AddMany(collection);
                    sw.Stop();
                    Console.WriteLine("Elapsed " + sw.Elapsed);
                    break;
                case 5:
                    await PrintSortedByF(collection);
                    sw.Stop();
                    Console.WriteLine("Elapsed " + sw.Elapsed);
                    break;
                default:
                    Console.WriteLine("Wrong param");
                    break;
            }
        }

        static async Task CreateExampleDBAsync(IMongoCollection<PeopleModel> collection) // Задание 1, добавляется 1 модель
        {
            var person = new PeopleModel { Surname = "ExampleSurname", Name = "ExampleName", LastName = "ExampleLastName", BirthDate = "05.06.2020", Sex = "m" };
            await collection.InsertOneAsync(person);
        }

        static async Task AddPersonAsync(
            IMongoCollection<PeopleModel> collection,
            string Surename,
            string Name,
            string LastName,
            string BirthDate,
            string Sex) // Задание 2, добавление 1 модели по заданным параметрам
        {
            var person = new PeopleModel { Surname = Surename, Name = Name, LastName = LastName, BirthDate = BirthDate, Sex = Sex };
            await collection.InsertOneAsync(person);
        }

        static async Task SortAsync(IMongoCollection<PeopleModel> collection) // задание 3, вывод уникальных строк
        {
            string temp = "-1";
            var users = await collection.Find("{}").Sort("{Unique:1}").ToListAsync();
            foreach (var user in users)
            {
                if (temp != user.Unique)
                    Print(user);
                temp = user.Unique;
            }
        }

        static async Task AddMany(IMongoCollection<PeopleModel> collection) // задание 4, запись миллиона строк в бд
        {
            List<PeopleModel> list = new List<PeopleModel>();

            for (int i = 0; i < 1000000; i++) //миллион рандомных строк
            {
                list.Add(new PeopleModel { Surname = GenerateName(6, false), Name = GenerateName(5, false), LastName = GenerateName(7, false), BirthDate = GenerateBirthDate(), Sex = GenerateSex() });
            }

            for (int i = 0; i < 100; i++) //100 строк с фамилией на F
            {
                list.Add(new PeopleModel { Surname = GenerateName(6, true), Name = GenerateName(5, false), LastName = GenerateName(7, false), BirthDate = GenerateBirthDate(), Sex = GenerateSex() });
            }

            await collection.InsertManyAsync(list);
        }

        public static string GenerateName(int len, bool fOnly)
        {
            string[] consonants = { "b", "c", "d", "f", "g", "h", "j", "k", "l", "m", "l", "n", "p", "q", "r", "s", "sh", "zh", "t", "v", "w", "x" };
            string[] vowels = { "a", "e", "i", "o", "u", "ae", "y" };
            string Name = "";
            if (fOnly)
                Name += "F";
            else
                Name += consonants[r.Next(consonants.Length)].ToUpper();
            Name += vowels[r.Next(vowels.Length)];
            int b = 2;
            while (b < len)
            {
                Name += consonants[r.Next(consonants.Length)];
                b++;
                Name += vowels[r.Next(vowels.Length)];
                b++;
            }

            return Name;
        }

        public static string GenerateBirthDate()
        {
            string result = r.Next(1, 30) + "." + r.Next(1, 12) + "." + r.Next(1900, 2022);
            return result;
        }

        public static string GenerateSex()
        {
            if (r.Next(2) == 0) return "m";
            else return "f";

        }

        static async Task PrintSortedByF(IMongoCollection<PeopleModel> collection) //задание 5, вывод людей с фамилией на F мужского пола
        {
            var filter = Builders<PeopleModel>
                .Filter
                .Regex("Surname", "F.*") &
                Builders<PeopleModel>
                .Filter
                .Regex("Sex", "m");
            var result = await collection.Find(filter).ToListAsync();
            PrintList(result);
        }



        static void PrintList(List<PeopleModel> models)
        {
            foreach (var model in models)
            {
                Console.WriteLine(model.Surname + " " + model.Name + " " + model.LastName + " Birth date: " + model.BirthDate + " Sex: " + model.Sex);
            }
        }

        static void Print(PeopleModel model)
        {
            Console.WriteLine(model.Surname + " " + model.Name + " " + model.LastName + " Birth date: " + model.BirthDate + " Sex: " + model.Sex);
        }
    }

    class PeopleModel
    {
        private string _unique;

        public ObjectId Id { get; set; }

        public string Surname { get; set; }

        public string Name { get; set; }

        public string LastName { get; set; }

        public string BirthDate { get; set; }

        public string Sex { get; set; }

        public string Unique
        {
            get { return Surname + Name + LastName + BirthDate; }
            set { _unique = value; }
        }
    }
}
