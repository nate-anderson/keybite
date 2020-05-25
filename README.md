![Keybite logo](keybite-text.png)

A (very beta) stateless key-value store with a cool API

## Tired of hosting servers for your serverless apps?
Hosting an RDS instance or database server for your simple serverless app? That sucks!

Keybite is a key-value store designed for use with serverless deployments (specifically AWS Lambda). While it currently only works with native filesystems, Keybite is designed to connect to run inside a Lambda function, connect to an S3 bucket and act as a (moderately) capable database for your serverless apps.

## Cool query syntax
Inspired by declarative query languages like MySQL and the conveniences of modern GraphQL APIs, Keybite's HTTP-based queries are flexible and straightforward. Simply POST a JSON object to the Keybite Lambda function to perform queries, inserts and updates all at once. Keybite's inline variable syntax even allows you to use query results before they are fetched. Check it out:

```json
{
  "userId": "insert users luke@skywalker.net",
  "userName": "insert_key user_name :userId Luke Skywalker"
}
```

Keybite lets you cut down on network calls and capitalize on Lambda's fast execution times to make your app's IO simple and fast.

## Auto-increment and map-style indexes
Keybite has auto-incrementing indexes (insert a value, get an automatically assigned ID) and map- or dictionary- style key-value storage (you provide the value _and_ the key).

## What keybite is
- A file-based key value store that doesn't require a server
- A flexible way of storing and maintaining data
- Limited and fragile

## What keybite isn't
- ACID compliant (but it wants to be!)
- A drop-in replacement for MySQL or MongoDB (no indexes based on value, no JSON parsing)
