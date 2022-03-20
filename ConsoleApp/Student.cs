using Newtonsoft.Json;
using System;

namespace ConsoleApp;

public class Student
{
    public string PartitionKey { get; set; }

    [JsonProperty(PropertyName = "id")]
    public string Id { get; set; }

    [JsonProperty(PropertyName = "_etag")]
    public string Etag { get; set; }

    public string Username { get; set; }

    public string FirstName { get; set; }

    public string LastName { get; set; }

    public Address Address { get; set; }

    public Course[] Courses { get; set; }

    public override string ToString()
    {
        return JsonConvert.SerializeObject(this);
    }
}

public class Course
{
    public string Name { get; set; }
    public string Trainer { get; set; }
    public DateTime StartDate { get; set; }
    public DateTime EndDate { get; set; }
}

public class Address
{
    public string Street { get; set; }
    public string City { get; set; }
    public string Country { get; set; }
}

