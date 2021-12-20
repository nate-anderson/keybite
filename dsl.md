Borrowing some ideas from GraphQL

The request object is a JSON object mapping variable names to query strings.
The response object is a JSON object mapping the same variable names to query results.

```js
// request
{
    "userId": "insert user user@example.com"
}
```

```js
// response
{
        "userId": 1,
}
```

Queries for which no result is desired can be mapped to "_"
If the client language spec or package does not allow duplicate JSON keys, unwanted
results can be mapped to keys starting with "_", such as the suggested "_0", "_1" etc.

Multiple queries and operations can be included in a single request

```js
// request
{
    "userId": "insert user user@example.com",
    "userName": "insert name Nate" 
}
```

```js
// response
{
    "userId": 1,
    "userName": 1,
}
```

Queries are executed sequentially
Queries can be chained such that results of earlier queries can be used as inputs to later queries

```js
// sequential request
{
    "userId": "insert user user@example.com",
    "nameId": "insert name Nate",
    "_": "insert user_name :userId :nameId"
}
```
