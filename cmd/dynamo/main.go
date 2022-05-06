package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strings"

	"github.com/TheWozard/goDynamoGraphClient/pkg/table"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/urfave/cli/v2"
)

const (
	TableFlag    = "table"
	EndpointFlag = "endpoint"
	RegionFlag   = "region"

	LimitFlag          = "limit"
	SourceKeyFlag      = "source"
	DestinationKeyFlag = "destination"
)

func main() {
	input := NewInput()
	app := &cli.App{
		Name:        "go-dynamo-graph-client",
		Description: "cli interface for interacting with the graph db",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    TableFlag,
				Aliases: []string{"t"},
				Value:   "example-table",
				Usage:   "Name of the table to work against",
			},
			&cli.StringFlag{
				Name:    EndpointFlag,
				Aliases: []string{"e"},
				Value:   "http://localhost:8000",
				Usage:   "Endpoint to hit for accessing the table",
			},
			&cli.StringFlag{
				Name:    RegionFlag,
				Aliases: []string{"r"},
				Value:   "us-east-1",
				Usage:   "Endpoint to hit for accessing the table",
			},
		},
		Commands: []*cli.Command{
			{
				Name:    "init",
				Aliases: []string{"i"},
				Usage:   "Creates a local table for working against. Will ask to delete table if one already exists.",
				Action: func(c *cli.Context) error {
					t := table.NewTable(BuildLocalClient(c), c.String(TableFlag))
					// Cleanup previous table
					if t.Exists() {
						if input.Confirm(fmt.Sprintf("Table '%s' already exists, would you like to delete and replace it?", t.Name)) {
							_, err := t.Delete()
							if err != nil {
								return err
							}
						} else {
							// If the table already exists we will get an error if we try to make another named it so just skip.
							// TODO: could we update a table? UpdateTable()
							return nil
						}
					}

					_, err := t.Create()

					return err
				},
			},
			{
				Name:    "read",
				Aliases: []string{"r"},
				Usage:   "Read entries out of the dynamo table",
				Flags: []cli.Flag{
					&cli.Int64Flag{
						Name:    LimitFlag,
						Aliases: []string{"l"},
						Value:   100,
						Usage:   "Limit of entries to return",
					},
					&cli.StringFlag{
						Name:    SourceKeyFlag,
						Aliases: []string{"s"},
						Usage:   "SourceKey to limit the results to",
					},
					&cli.StringFlag{
						Name:    DestinationKeyFlag,
						Aliases: []string{"d"},
						Usage:   "DestinationKey to limit the results to",
					},
				},
				Action: func(c *cli.Context) error {
					t := table.NewTable(BuildLocalClient(c), c.String(TableFlag))

					if !t.Exists() {
						fmt.Printf("Could not locate table '%s'", t.Name)
						return nil
					}

					total := 0
					limit := c.Int64(LimitFlag)
					return t.ReadWalker(table.ReadWalkerInput{
						PageSize: limit,
					}, func(rwo table.ReadWalkerOutput, b bool) bool {
						for _, item := range rwo.Items {
							fmt.Println(table.WriteItem(item))
						}
						total += int(rwo.Count)
						fmt.Printf("Total: %d rows\n", total)
						return rwo.Count < limit || input.Continue()
					})
				},
			},
			{
				Name:    "load",
				Aliases: []string{"l"},
				Usage:   "loads arguments as csv files into the db",
				Action: func(c *cli.Context) error {
					t := table.NewTable(BuildLocalClient(c), c.String(TableFlag))

					if !t.Exists() {
						fmt.Printf("Could not locate table '%s'", t.Name)
						return nil
					}

					total := 0

					for _, arg := range c.Args().Slice() {
						file, err := os.Open(arg)
						if err != nil {
							log.Fatal(err)
						}
						r := csv.NewReader(file)
						headers, err := r.Read()
						if err != nil {
							log.Fatal(err)
						}
						sourceIndex := IndexOf(headers, "source")
						targetIndex := IndexOf(headers, "target")
						for {
							record, err := r.Read()
							if err == io.EOF {
								break
							}
							if err != nil {
								log.Fatal(err)
							}
							item := map[string]*dynamodb.AttributeValue{
								table.SourceKeyAttribute: {S: aws.String(record[sourceIndex])},
								table.TargetKeyAttribute: {S: aws.String(record[targetIndex])},
							}
							for i, value := range record {
								if i != sourceIndex && i != targetIndex {
									item[headers[i]] = &dynamodb.AttributeValue{S: aws.String(value)}
								}
							}
							_, err = t.Put(item)
							if err != nil {
								return err
							}
							total++
						}
					}
					fmt.Printf("Loaded %d records", total)
					return nil
				},
			},
		},
	}

	sort.Sort(cli.FlagsByName(app.Flags))
	sort.Sort(cli.CommandsByName(app.Commands))

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

// BuildLocalClient build a *dynamodb.DynamoDB for interacting with a local dynamodb
func BuildLocalClient(context *cli.Context) *dynamodb.DynamoDB {
	session, err := session.NewSession(aws.NewConfig().WithRegion(context.String(RegionFlag)).WithEndpoint(context.String(EndpointFlag)))
	if err != nil {
		log.Fatal(err)
	}
	return dynamodb.New(session)
}

// NewInput creates a new Input for interacting with the user
func NewInput() Input {
	return Input{
		reader: bufio.NewReader(os.Stdin),
	}
}

// Input helper struct for handling common interactions with the user
type Input struct {
	reader *bufio.Reader
}

// Confirm writes the message to the user and ways for yes/no response
func (i Input) Confirm(message string) bool {
	fmt.Printf("%s (y/N) ", message)
	text, err := i.reader.ReadString('\n')
	if err != nil {
		log.Fatal(err)
	}
	lower := strings.ToLower(text)
	return lower == "y" || lower == "yes"
}

// Pauses until the user pressed enter. Will exit when anything else is returned
func (i Input) Continue() bool {
	fmt.Printf("Press 'Enter' to continue")
	// TODO: This could be improved to instantly exist when the user presses any key other then enter
	text, err := i.reader.ReadString('\n')
	if err != nil {
		log.Fatal(err)
	}
	trimmed := strings.TrimSpace(text)
	return trimmed == ""
}

func IndexOf(slice []string, item string) int {
	for i, data := range slice {
		if data == item {
			return i
		}
	}
	return -1
}
