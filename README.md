![Keybite logo](keybite-text.png)

A very alpha stateless key-value store with a cool API

## Tired of hosting database servers for your serverless apps?
Hosting an RDS instance or database server for your simple serverless app? That sucks!

Keybite is a key-value store designed for use with serverless apps (specifically AWS Lambda). Keybite is designed to run as a Lambda function, connect to an S3 bucket and act as a (moderately) capable database for your serverless apps.

## Cool, simple query language
Inspired by declarative query languages like SQL and the conveniences of modern GraphQL APIs, Keybite's HTTP-based queries are flexible and straightforward. Simply POST a JSON object to the Keybite Lambda function to perform queries, inserts and updates all at once. Keybite's inline variable syntax even allows you to use query results before they are fetched. Check it out:

```json
{
  "userId": "insert users luke@skywalker.net",
  "userName": "insert_key user_name :userId Luke Skywalker"
}
```

You just provide a JSON object with a list of variables and statements used to populate them.

Keybite lets you cut down on network calls and capitalize on Lambda's fast execution times to make your app's IO simple and fast.

## Auto-increment and map-style indexes
Keybite has auto-incrementing indexes (insert a value, get an automatically assigned ID) and map- or dictionary- style key-value storage (you provide the value _and_ the key).

## Flexible Use Options
Keybite can be served as an HTTP API or an AWS Lambda function, and can be used locally via command line interface. It also comes with storage drivers for a local
filesystem and an S3 bucket.

# Why?
I liked the idea of being able to make simple app prototypes without paying for database server uptime. I also wanted to write a key-value store
and design a query language. I don't promise this will be useful to anyone. I haven't done the math and can't be sure it's actually
cheaper than just firing up an EC2 or RDS instance with MySQL.

## What keybite is
- A file-based key value store that doesn't require an always-on server
- A flexible way of storing and maintaining data
- Great for prototyping and small personal projects
- Limited and fragile (see index notes above; limited key string length)

## What keybite isn't
- A drop-in replacement for MySQL or MongoDB (no indexes based on value, no JSON parsing)
- Suited for analytics tasks for the same reasons as above
- Secure (don't make queries straight from a frontend, don't store sensitive data)
- Production-ready


## What keybite will be
- Concurrency safe (it tries to be, but I'm not positive it is)
