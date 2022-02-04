package main

import (
	"context"
	"fmt"
	awsSdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	athenaTypes "github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/flyteorg/flyteplugins/go/tasks/aws"
	"time"
)

func main() {
	awsConfig := aws.Config{
		Region:    "us-east-1",
		AccountID: "123123123123",
		Retries:   3,
		LogLevel:  0,
	}

	sdkCfg, err := awsConfig.GetSdkConfig()
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	client := athena.NewFromConfig(sdkCfg)

	resp, err := client.StartQueryExecution(ctx,
		&athena.StartQueryExecutionInput{
			ClientRequestToken: awsSdk.String("request-token-1111111111111111111111111"),
			QueryExecutionContext: &athenaTypes.QueryExecutionContext{
				Database: awsSdk.String("vaccinations"),
				Catalog:  awsSdk.String("AwsDataCatalog"),
			},
			ResultConfiguration: &athenaTypes.ResultConfiguration{
				OutputLocation: awsSdk.String("s3://flyte-demo/athena-test"),
			},
			QueryString: awsSdk.String("select * from vaccinations limit 10"),
		})

	if err != nil {
		panic(err)
	}

	fmt.Println(resp.QueryExecutionId)
	qri := &athena.GetQueryExecutionInput{
		QueryExecutionId: resp.QueryExecutionId,
	}

	var qrop *athena.GetQueryExecutionOutput
	duration := time.Duration(5) * time.Second

	for {
		qrop, err = client.GetQueryExecution(ctx, qri)
		if err != nil {
			fmt.Println(err)
			return
		}
		if qrop.QueryExecution.Status.State != "RUNNING" {
			break
		}
		fmt.Println("waiting.")
		time.Sleep(duration)
	}

	if qrop.QueryExecution.Status.State == "SUCCEEDED" {
		ip := athena.GetQueryResultsInput{
			QueryExecutionId: resp.QueryExecutionId,
		}
		op, err := client.GetQueryResults(ctx, &ip)
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Printf("%+v", op.ResultSet.Rows)
	} else {
		fmt.Println(qrop.QueryExecution.Status.State)
	}
}
