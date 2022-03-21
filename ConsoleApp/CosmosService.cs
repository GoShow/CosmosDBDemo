using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Fluent;
using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using Microsoft.Azure.Cosmos.Linq;
using Microsoft.Azure.Cosmos.Scripts;
using Newtonsoft.Json;

namespace ConsoleApp;

public class CosmosService
{
    //private const string EndpointUri = "<YOUR URI>";
    //private const string PrimaryKey = "<YOUR PRIMARY KEY>";

    //Local Settings:
    private const string EndpointUri = "https://localhost:8081";
    private const string PrimaryKey = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";

    private const string DatabaseId = "softuni";
    private const string ContainerId = "education";

    public async Task ExecuteCommand(string command)
    {
        using (var cosmosClient = new CosmosClientBuilder(EndpointUri, PrimaryKey).WithApplicationRegion(Regions.WestEurope).Build())
        {
            switch (command)
            {
                case "1":
                    await CreateDatabaseAsync(cosmosClient);
                    break;
                case "2":
                    await CreateContainerAsync(cosmosClient);
                    break;
                case "3":
                    await ScaleContainerAsync(cosmosClient);
                    break;
                case "4":
                    await AddItemsAsync(cosmosClient);
                    break;
                case "5":
                    await QueryItemsAsync(cosmosClient);
                    break;
                case "6":
                    await UpdateItemAsync(cosmosClient);
                    break;
                case "7":
                    await DeleteItemAsync(cosmosClient);
                    break;
                case "8":
                    await ConcurrencyUpdateAsync(cosmosClient);
                    break;
                case "9":
                    await ConcurrencyUpdateWithEtagAsync(cosmosClient);
                    break;
                case "10":
                    await ProcessTransactionAsync(cosmosClient);
                    break;
                case "11":
                    await ProcessTransactionAsync(cosmosClient, shouldFail: true);
                    break;
                case "12":
                    await BulkInsertAsync(cosmosClient);
                    break;
                case "13":
                    await QueryItemsWithContinuationTokenAsync(cosmosClient);
                    break;
                case "14":
                    await ExecuteStoredProcedureAsync(cosmosClient);
                    break;
                case "15":
                    await DeleteDatabaseAsync(cosmosClient);
                    break;
                default:
                    Console.WriteLine($"Invalid command: {command}");
                    break;
            }
        }
    }

    private async Task CreateDatabaseAsync(CosmosClient cosmosClient)
    {
        DatabaseResponse response = await cosmosClient.CreateDatabaseIfNotExistsAsync(DatabaseId);

        Console.WriteLine($"Created Database: {response.Database.Id}\n");
    }

    private async Task CreateContainerAsync(CosmosClient cosmosClient)
    {
        Database database = cosmosClient.GetDatabase(DatabaseId);

        ContainerResponse response = await database.CreateContainerIfNotExistsAsync(ContainerId, "/PartitionKey");

        Console.WriteLine($"Created Container: {response.Container.Id}\n");
    }

    private async Task ScaleContainerAsync(CosmosClient cosmosClient)
    {
        try
        {
            Container container = cosmosClient.GetContainer(DatabaseId, ContainerId);

            int? throughput = await container.ReadThroughputAsync();
            if (throughput.HasValue)
            {
                Console.WriteLine($"Current provisioned throughput : {throughput.Value}\n");

                int newThroughput = 10000;

                await container.ReplaceThroughputAsync(newThroughput);

                Console.WriteLine($"New provisioned throughput : {newThroughput}\n");
            }
        }
        catch (CosmosException cosmosException) when (cosmosException.StatusCode == HttpStatusCode.BadRequest)
        {
            Console.WriteLine("Cannot read container throughput.");
            Console.WriteLine(cosmosException.ResponseBody);
        }
    }

    private async Task AddItemsAsync(CosmosClient cosmosClient)
    {
        Container container = cosmosClient.GetContainer(DatabaseId, ContainerId);

        var jonSkeet = new Student
        {
            Id = "8a4e944e-061c-4daf-9daf-d062f1234dd5",
            PartitionKey = "USA",
            Username = "jon.skeet",
            FirstName = "Jon",
            LastName = "Skeet",
            Address = new Address { Street = "5th Avenue", City = "New York", Country = "USA" },
            Courses = new Course[]
            {
                new Course
                {
                    Name = "C# Basics",
                    Trainer = "Svetlin Nakov",
                    StartDate = DateTime.UtcNow,
                    EndDate = DateTime.UtcNow.AddMonths(1)
                },
                new Course
                {
                    Name = "C# Fundamentials",
                    Trainer = "Svetlin Nakov",
                    StartDate = DateTime.UtcNow.AddMonths(2),
                    EndDate = DateTime.UtcNow.AddMonths(3)
                }
            }
        };

        ItemResponse<Student> jonSkeetResponse =
            await container.CreateItemAsync<Student>(jonSkeet, new PartitionKey(jonSkeet.PartitionKey));

        Console.WriteLine($"Created item in database with id: {jonSkeetResponse.Resource.Id}");

        //Use of UpsertItemAsync method
        //ItemResponse<Student> jonSkeetResponse =
        //    await container.UpsertItemAsync<Student>(jonSkeet, new PartitionKey(jonSkeet.Address.Country));

        //Console.WriteLine($"Created/Updated item in database with id: {jonSkeetResponse.Resource.Id}");

        Console.WriteLine($"Operation consumed {jonSkeetResponse.RequestCharge} RUs.\n");

        var bjarneStroustrup = new Student
        {
            Id = "1669e6aa-b695-4346-9985-8b6c81a33b26",
            PartitionKey = "Denmark",
            Username = "bjarne.stroustrup",
            FirstName = "Bjarne",
            LastName = "Stroustrup",
            Address = new Address { Street = "Solstien", City = "Aarhus", Country = "Denmark" },
            Courses = new Course[]
            {
                new Course
                {
                    Name = "C++ Basics",
                    Trainer = "George Georgiev",
                    StartDate = DateTime.UtcNow,
                    EndDate = DateTime.UtcNow.AddMonths(1)
                }
            }
        };

        ItemResponse<Student> bjarneStroustrupResponse =
            await container.CreateItemAsync<Student>(bjarneStroustrup, new PartitionKey(bjarneStroustrup.PartitionKey));

        Console.WriteLine($"Created item in database with id: {bjarneStroustrupResponse.Resource.Id}");
        Console.WriteLine($"Operation consumed {bjarneStroustrupResponse.RequestCharge} RUs.\n");

        var robertMartin = new Student
        {
            Id = "7a1ec9a9-b6f7-437f-a541-f36c29282816",
            PartitionKey = "USA",
            Username = "robert.martin",
            FirstName = "Robert",
            LastName = "Martin",
            Address = new Address { Street = "Sunset Blvd.", City = "Los Angeles", Country = "USA" },
            Courses = new Course[]
            {
                new Course
                {
                    Name = "Clojure Basics",
                    Trainer = "Angel Georgiev",
                    StartDate = DateTime.UtcNow,
                    EndDate = DateTime.UtcNow.AddMonths(1)
                }
            }
        };

        ItemResponse<Student> robertMartinResponse =
            await container.CreateItemAsync<Student>(robertMartin, new PartitionKey(robertMartin.PartitionKey));

        Console.WriteLine($"Created item in database with id: {robertMartinResponse.Resource.Id}");
        Console.WriteLine($"Operation consumed {robertMartinResponse.RequestCharge} RUs.\n");
    }

    private async Task QueryItemsAsync(CosmosClient cosmosClient)
    {
        Container container = cosmosClient.GetContainer(DatabaseId, ContainerId);

        string sqlQueryText = "SELECT * FROM c WHERE c.PartitionKey = 'USA'";

        Console.WriteLine($"Running query: {sqlQueryText}\n");

        QueryDefinition queryDefinition = new QueryDefinition(sqlQueryText);

        List<Student> students = new();

        using FeedIterator<Student> feedIterator = container.GetItemQueryIterator<Student>(queryDefinition);

        double requestUnits = 0;
        while (feedIterator.HasMoreResults)
        {
            FeedResponse<Student> response = await feedIterator.ReadNextAsync();
            students.AddRange(response);
            requestUnits += response.RequestCharge;
        }

        foreach (Student student in students)
        {
            Console.WriteLine($"{student}\n");
        }

        Console.WriteLine($"Queried items: {students.Count}");
        Console.WriteLine($"Request units: {requestUnits}\n");
        Console.WriteLine("Press any key to continue..");
        Console.ReadKey();
        Console.Clear();
    }

    private async Task UpdateItemAsync(CosmosClient cosmosClient)
    {
        Container container = cosmosClient.GetContainer(DatabaseId, ContainerId);

        ItemResponse<Student> getResponse =
            await container.ReadItemAsync<Student>("1669e6aa-b695-4346-9985-8b6c81a33b26", new PartitionKey("Denmark"));

        Student student = getResponse.Resource;

        student.Address.Street = "Todor Kableshkov";

        //Note: UpsertItemAsync could be used
        ItemResponse<Student> updateResponse =
            await container.ReplaceItemAsync<Student>(
                student,
                student.Id,
                new PartitionKey(student.PartitionKey),
                new ItemRequestOptions { IfMatchEtag = student.Etag });

        Console.WriteLine($"Updated Student: {updateResponse.Resource}\n");
    }

    private async Task DeleteItemAsync(CosmosClient cosmosClient)
    {
        Container container = cosmosClient.GetContainer(DatabaseId, ContainerId);
        string studentId = "1669e6aa-b695-4346-9985-8b6c81a33b26";

        ItemResponse<Student> response = await container.DeleteItemAsync<Student>(studentId, new PartitionKey("Denmark"));
        Console.WriteLine($"Deleted Student with id: {studentId}\n");
    }

    private async Task ConcurrencyUpdateAsync(CosmosClient cosmosClient)
    {
        Console.WriteLine("Starting concurrency update.");
        Container container = cosmosClient.GetContainer(DatabaseId, ContainerId);

        Task task1 = FirstUpdate();
        Task task2 = SecondUpdate();

        await Task.WhenAll(task1, task2);

        Console.WriteLine("Completed.\n");

        async Task FirstUpdate()
        {
            ItemResponse<Student> response =
                await container.ReadItemAsync<Student>("8a4e944e-061c-4daf-9daf-d062f1234dd5", new PartitionKey("USA"));

            Student student = response.Resource;

            student.FirstName = "Georgi";

            Thread.Sleep(3000);

            ItemResponse<Student> updateResponse =
                await container.ReplaceItemAsync<Student>(student, student.Id, new PartitionKey(student.PartitionKey));
        }
        async Task SecondUpdate()
        {
            ItemResponse<Student> response =
                await container.ReadItemAsync<Student>("8a4e944e-061c-4daf-9daf-d062f1234dd5", new PartitionKey("USA"));

            Student student = response.Resource;

            student.LastName = "Inkov";

            ItemResponse<Student> updateResponse =
                await container.ReplaceItemAsync<Student>(student, student.Id, new PartitionKey(student.PartitionKey));
        }
    }

    private async Task ConcurrencyUpdateWithEtagAsync(CosmosClient cosmosClient)
    {
        Container container = cosmosClient.GetContainer(DatabaseId, ContainerId);

        Console.WriteLine("Starting concurrency update with Etag.");

        try
        {
            Task task1 = FirstUpdate();
            Task task2 = SecondUpdate();

            await Task.WhenAll(task1, task2);
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.Message);
        }
        finally
        {
            Console.WriteLine("Completed.\n");
        }

        async Task FirstUpdate()
        {
            ItemResponse<Student> response =
                await container.ReadItemAsync<Student>("8a4e944e-061c-4daf-9daf-d062f1234dd5", new PartitionKey("USA"));

            Thread.Sleep(3000);

            Student student = response.Resource;

            student.FirstName = "Georgi";

            ItemResponse<Student> updateResponse =
                await container.ReplaceItemAsync<Student>(
                    student,
                    student.Id,
                    new PartitionKey(student.PartitionKey),
                    new ItemRequestOptions { IfMatchEtag = student.Etag });
        }
        async Task SecondUpdate()
        {
            ItemResponse<Student> response =
                await container.ReadItemAsync<Student>("8a4e944e-061c-4daf-9daf-d062f1234dd5", new PartitionKey("USA"));

            Student student = response.Resource;

            student.LastName = "Inkov";

            ItemResponse<Student> updateResponse =
                await container.ReplaceItemAsync<Student>(
                    student,
                    student.Id,
                    new PartitionKey(student.PartitionKey),
                    new ItemRequestOptions { IfMatchEtag = student.Etag });
        }
    }

    private async Task ProcessTransactionAsync(CosmosClient cosmosClient, bool shouldFail = false)
    {
        Container container = cosmosClient.GetContainer(DatabaseId, ContainerId);

        var pesho = new Student
        {
            Id = Guid.NewGuid().ToString(),
            PartitionKey = "Bulgaria",
            Username = "p.peshev",
            FirstName = "Pesho",
            LastName = "Peshev",
            Address = new Address { Street = "A. Malinov", City = "Sofia", Country = "Bulgaria" }
        };

        var gosho = new Student
        {
            // If shouldFail is true use previous object's Id - this will return error Conflict for trying to create 2 documents with same Id, else use new GUID
            Id = shouldFail ? pesho.Id : Guid.NewGuid().ToString(),
            PartitionKey = "Bulgaria",
            Username = "g.poshev",
            FirstName = "Gosho",
            LastName = "Goshev",
            Address = new Address { Street = "T. Kableshkov", City = "Sofia", Country = "Bulgaria" }
        };

        TransactionalBatch batch = container.CreateTransactionalBatch(new PartitionKey("Bulgaria"))
          .CreateItem<Student>(pesho)
          .CreateItem<Student>(gosho);

        TransactionalBatchResponse batchResponse = await batch.ExecuteAsync();

        using (batchResponse)
        {
            TransactionalBatchOperationResult<Student> peshoResult = batchResponse.GetOperationResultAtIndex<Student>(0);
            TransactionalBatchOperationResult<Student> goshoResult = batchResponse.GetOperationResultAtIndex<Student>(1);

            if (batchResponse.IsSuccessStatusCode)
            {
                Console.WriteLine($"Transaction successful. Items processed: {batchResponse.Count}\n");
            }
            else
            {
                Console.WriteLine($"Transaction failed. Status Code: {batchResponse.StatusCode}\n");
            }
        }
    }

    private async Task BulkInsertAsync(CosmosClient cosmosClient)
    {
        int insertCount = 2000;

        Container container = cosmosClient.GetContainer(DatabaseId, ContainerId);

        var partitionKeys = new string[] { "USA", "England", "Denmark", "Bulgaria" };

        IReadOnlyCollection<Student> students = new Bogus.Faker<Student>()
            .RuleFor(s => s.PartitionKey, f => f.Random.ArrayElement(partitionKeys))
            .RuleFor(s => s.Id, f => f.Random.Guid().ToString())
            .RuleFor(s => s.Username, f => f.Internet.UserName())
            .RuleFor(s => s.FirstName, f => f.Name.FirstName())
            .RuleFor(s => s.LastName, f => f.Name.LastName())
            .RuleFor(
                s => s.Address,
                f => new Address
                {
                    Street = f.Address.StreetName(),
                    City = f.Address.City(),
                    Country = f.Address.Country()
                })
            .RuleFor(s => s.Courses, f => new Course[] { })
            .Generate(insertCount);

        List<Task> tasks = new List<Task>(insertCount);

        Console.WriteLine("Bulk Insert in progress...");

        double requestUnits = 0;
        foreach (Student student in students)
        {
            tasks.Add(container.CreateItemAsync(student, new PartitionKey(student.PartitionKey))
                .ContinueWith(x => { requestUnits += x.Result.RequestCharge; }));
        }

        await Task.WhenAll(tasks);

        Console.WriteLine($"Inserted items: {students.Count}");
        Console.WriteLine($"Request units: {requestUnits}\n");
        Console.WriteLine("Press any key to continue..");
        Console.ReadKey();
        Console.Clear();
    }

    private async Task QueryItemsWithContinuationTokenAsync(CosmosClient cosmosClient, string continuationToken = null)
    {
        Container container = cosmosClient.GetContainer(DatabaseId, ContainerId);

        List<Student> students = new();

        int maxItemsCount = 10;

        //Using LINQ
        IQueryable<Student> query =
            from model in container.GetItemLinqQueryable<Student>(
                false,
                continuationToken,
                new QueryRequestOptions { PartitionKey = new PartitionKey("USA"), MaxItemCount = maxItemsCount })
            where model.FirstName.StartsWith('A')
            select model;

        using FeedIterator<Student> feedIterator = (query).ToFeedIterator();

        FeedResponse<Student> response = await feedIterator.ReadNextAsync();
        students.AddRange(response);
        continuationToken = response.ContinuationToken;

        foreach (Student student in students)
        {
            Console.WriteLine($"{student}\n");
        }

        Console.WriteLine($"Queried items: {students.Count}");
        Console.WriteLine($"Request units: {response.RequestCharge}\n");
        Console.WriteLine($"Press 'n' for next {maxItemsCount} items or any other key to quit.");

        if (continuationToken is null)
        {
            Console.WriteLine("All items were queried\n");
            Console.WriteLine("Press any key to continue");
            Console.ReadKey();
            Console.Clear();

            return;
        }
        if (Console.ReadKey().KeyChar is 'n')
        {
            await QueryItemsWithContinuationTokenAsync(cosmosClient, continuationToken);
        }
        else
        {
            Console.Clear();
        }
    }

    private async Task ExecuteStoredProcedureAsync(CosmosClient cosmosClient, string continuationToken = null)
    {
        //https://cosmosdb.github.io/labs/dotnet/labs/06-multi-document-transactions.html
        Container container = cosmosClient.GetContainer(DatabaseId, ContainerId);

        StoredProcedureExecuteResponse<string> response =
            await container.Scripts.ExecuteStoredProcedureAsync<string>("DemoSP", new PartitionKey("USA"), null);

        IEnumerable<Student> students = JsonConvert.DeserializeObject<IEnumerable<Student>>(response.Resource);

        foreach (var student in students)
        {
            Console.WriteLine(student);
            Console.WriteLine();
        }

        Console.WriteLine($"Queried items: {students.Count()}");
        Console.WriteLine($"Request units: {response.RequestCharge}\n");
        Console.WriteLine("Press any key to continue");
        Console.ReadKey();
        Console.Clear();
    }

    private async Task DeleteDatabaseAsync(CosmosClient cosmosClient)
    {
        Database database = cosmosClient.GetDatabase(DatabaseId);

        DatabaseResponse response = await database.DeleteAsync();

        Console.WriteLine($"Deleted Database: {response.Database.Id}\n");
    }
}


