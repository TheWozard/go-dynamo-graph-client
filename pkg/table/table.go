package table

import (
	"fmt"
	"sort"
	"strings"

	"github.com/TheWozard/goDynamoGraphClient/pkg/common"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

const (
	TargetKeyAttribute = "target-key"
	SourceKeyAttribute = "source-key"
	TimestampAttribute = "timestamp"
	TagAttribute       = "tag"
	DataAttribute      = "data"
)

// Service interface that matches *dynamodb.DynamoDB but can be replaced for unit-test.
type Service interface {
	CreateTable(*dynamodb.CreateTableInput) (*dynamodb.CreateTableOutput, error)
	DescribeTable(input *dynamodb.DescribeTableInput) (*dynamodb.DescribeTableOutput, error)
	DeleteTable(*dynamodb.DeleteTableInput) (*dynamodb.DeleteTableOutput, error)
	// QueryPages(input *dynamodb.QueryInput, fn func(*dynamodb.QueryOutput, bool) bool) error
	ScanPages(input *dynamodb.ScanInput, fn func(*dynamodb.ScanOutput, bool) bool) error
	PutItem(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error)
}

// NewTable creates a new table for the passed *dynamodb.DynamoDB
func NewTable(svc *dynamodb.DynamoDB, name string) Table {
	return Table{
		Name: name,
		svc:  svc,
	}
}

// Table wrapper to apply common functions to a table
type Table struct {
	Name string
	svc  Service
}

// Exists returns if a table already exists
func (t Table) Exists() bool {
	_, err := t.svc.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: aws.String(t.Name),
	})
	return err == nil
}

// Create creates a table ready for storing graph data in dynamo
func (t Table) Create() (*dynamodb.CreateTableOutput, error) {
	return t.svc.CreateTable(&dynamodb.CreateTableInput{
		TableName: aws.String(t.Name),
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String(SourceKeyAttribute),
				AttributeType: aws.String("S"),
			},
			{
				AttributeName: aws.String(TargetKeyAttribute),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String(SourceKeyAttribute),
				KeyType:       aws.String("HASH"),
			},
			{
				AttributeName: aws.String(TargetKeyAttribute),
				KeyType:       aws.String("RANGE"),
			},
		},
		BillingMode: aws.String(dynamodb.BillingModePayPerRequest), // TODO: this should be configurable when not local
		GlobalSecondaryIndexes: []*dynamodb.GlobalSecondaryIndex{
			{
				IndexName: aws.String("reverse-search"),
				KeySchema: []*dynamodb.KeySchemaElement{
					{
						AttributeName: aws.String(TargetKeyAttribute),
						KeyType:       aws.String("HASH"),
					},
					{
						AttributeName: aws.String(SourceKeyAttribute),
						KeyType:       aws.String("RANGE"),
					},
				},
				Projection: &dynamodb.Projection{
					ProjectionType: aws.String(dynamodb.ProjectionTypeKeysOnly),
				},
			},
		},
	})
}

// Delete deletes the current table
func (t Table) Delete() (*dynamodb.DeleteTableOutput, error) {
	return t.svc.DeleteTable(&dynamodb.DeleteTableInput{
		TableName: aws.String(t.Name),
	})
}

// ReadWalkerInput input for the Table.ReadWalk function
type ReadWalkerInput struct {
	// Page size per iteration of the walk function
	PageSize int64 `validate:"gte=0,lte=1000"`
	// TODO: SourceKey DestinationKey
}

type ReadWalkerOutput struct {
	Count int64
	Items []map[string]*dynamodb.AttributeValue
}

// Limit configured Limit with default
func (rwi ReadWalkerInput) Limit() int64 {
	if rwi.PageSize != 0 {
		return rwi.PageSize
	}
	return 100
}

// ReadWalk generically "Reads" data from the dynamo table.
// This will either query for specific data if passed or do a general scan when no specific data is specified
func (t Table) ReadWalker(input ReadWalkerInput, walk func(rwo ReadWalkerOutput, b bool) bool) error {
	err := common.Validate.Struct(input)
	if err != nil {
		return err
	}
	return t.svc.ScanPages(&dynamodb.ScanInput{
		TableName: aws.String(t.Name),
		Limit:     aws.Int64(input.Limit()),
	}, func(so *dynamodb.ScanOutput, b bool) bool {
		return walk(ReadWalkerOutput{
			Count: *so.Count,
			Items: so.Items,
		}, b)
	})
}

func (t Table) Put(item map[string]*dynamodb.AttributeValue) (*dynamodb.PutItemOutput, error) {
	return t.svc.PutItem(&dynamodb.PutItemInput{
		TableName: aws.String(t.Name),
		Item:      item,
	})
}

func WriteItem(item map[string]*dynamodb.AttributeValue) string {
	source, target := *item[SourceKeyAttribute].S, *item[TargetKeyAttribute].S
	extra := []string{}
	keys := []string{}
	for key := range item {
		if key != SourceKeyAttribute && key != TargetKeyAttribute {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	for _, key := range keys {
		extra = append(extra, fmt.Sprintf("%s:%s", key, *item[key].S))
	}
	if source == target {
		return fmt.Sprintf("[%s] %s", source, strings.Join(extra, " "))
	}
	return fmt.Sprintf("%s -> %s %s", source, target, strings.Join(extra, " "))
}
