![Keybite logo](keybite-text.png)

A very alpha stateless key-value store with a cool API

## A serverless key-value store with teeth
Hosting an RDS instance or database server for your simple serverless app sucks. Keybite can live inside a lambda function and persist its data in S3, meaning it costs practically nothing when sitting idle.

## Cool, simple query language
Inspired by declarative query languages like SQL and the conveniences of modern GraphQL APIs, Keybite's HTTP-based queries are flexible and straightforward. Simply POST a JSON object to the Keybite Lambda function to perform queries, inserts and updates all at once. Keybite's inline variable syntax even allows you to use query results before they are fetched.

```json
{
  "userId": "insert users luke@skywalker.net",
  "userName": "insert_key user_name :userId Luke Skywalker"
}
```

You just provide a JSON object with a list of variables and statements used to populate them. A single HTTP request to Keybite can contain as many queries as you need, and you can reference query results before they are resolved.

## Auto-increment and map-style indexes
Keybite has auto-incrementing indexes (insert a value, get an automatically assigned ID) and map- or dictionary- style key-value storage (you provide the value _and_ the key).

## Flexible Use Options
Keybite can be run as a standalone HTTP server or as a Lambda function. Data can be stored in a local filesystem when run as a standalone HTTP server, or in an EFS volume or S3 bucket when running as a Lambda function.

# Why?
I wanted to better understand NoSQL databases and hash tables, and try my hand at designing a database API and query DSL, while building something useful (at least to myself). I liked the idea of being able to deploy and test simple app prototypes without paying for database server uptime.

I don't promise this will be useful to anyone. I don't promise it will actually save you money, and I certainly don't promise it won't blow up under an important workload.

## What keybite is
- A file-based key value store that doesn't require an always-on server
- A flexible way of storing and maintaining data
- Great for prototyping and small personal projects
- Limited and fragile (see index notes above; limited key string length)

## What keybite isn't
- A replacement for a full-fledged RDBMS or NoSQL database (no lookups by value, no JSON parsing or type validation)
- Suited for analytics tasks for the same reasons as above
- Secure (don't make queries straight from a frontend, don't store sensitive data)
- Production-ready


## What keybite will be
- Safe for concurrent use (it tries to be, but I'm not positive it is)
