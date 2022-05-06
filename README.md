# go-dynamo-graph-client
[![Test Status](https://github.com/TheWozard/go-dynamo-graph-client/actions/workflows/test.yml/badge.svg)](https://github.com/TheWozard/go-dynamo-graph-client/actions/workflows/test.yml)
[![Coverage Status](https://coveralls.io/repos/github/TheWozard/go-dynamo-graph-client/badge.svg?branch=master)](https://coveralls.io/github/TheWozard/go-dynamo-graph-client?branch=master)

A golang client using dynamo db as an (Adjacency List)[https://en.wikipedia.org/wiki/Adjacency_list] graph

# Motivation

When working with Neptune, for high volume but shallow relationships the idea to use an Adjacency List and dynamodb seemed like a much better approach and would not run into the same level of write conflicts from Neptune. When the number of hops in a given search is low, dynamodb's low latency can make this viable.

# Running Locally
Current version only works against local tables
```
make local-up
```
Uses docker-compose to standup a persistent dynamodb instance on `localhost:8000`

```
go run ./cmd/dynamo/main.go init
go run ./cmd/dynamo/main.go load ./example/basic.csv
```
`init` creates the table on the local instance  
`load` loads the table with the csv file raw data into the db

To scan through the local table
```
go run ./cmd/dynamo/main.go read
```

For more details
```
go run ./cmd/dynamo/main.go help
```