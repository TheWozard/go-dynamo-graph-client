# go-dynamo-graph-client

A golang client using dynamo db as an (Adjacency List)[https://en.wikipedia.org/wiki/Adjacency_list] graph

# Motivation

When working with Neptune, for high volume but shallow relationships the idea to use an Adjacency List and dynamodb seemed like a much better approach and would not run into the same level of write conflicts from Neptune. When the number of hops in a given search is low, dynamodb's low latency can make this viable

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
`load` loads the table with the passed csv file raw into the db

```
go run ./cmd/dynamo/main.go read
```
lets you scan through the local table

For more details
```
go run ./cmd/dynamo/main.go help
```