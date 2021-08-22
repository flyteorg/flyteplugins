package snowflake

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"testing"
)

func TestCreateTaskInfo(t *testing.T) {
	t.Run("create task info", func(t *testing.T) {
		taskInfo := createTaskInfo("d5493e36", "test-account")

		assert.Equal(t, 1, len(taskInfo.Logs))
		assert.Equal(t, taskInfo.Logs[0].Uri, "https://test-account.snowflakecomputing.com/console#/monitoring/queries/detail?queryId=d5493e36")
		assert.Equal(t, taskInfo.Logs[0].Name, "Snowflake Console")
	})
}

func TestBuildRequest(t *testing.T) {
	account := "test-account"
	token := "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9"
	queryId := "019e70eb-0000-278b-0000-40f100012b1a"
	snowflakeUrl := "https://" + account + ".snowflakecomputing.com/api/statements"
	t.Run("build http request for submitting a snowflake query", func(t *testing.T) {
		queryInfo := QueryInfo{
			Account:   account,
			Warehouse: "test-warehouse",
			Schema:    "test-schema",
			Database:  "test-database",
			Statement: "SELECT 1",
		}

		req, err := buildRequest("POST", queryInfo, account, token, queryId, false)
		header := http.Header{}
		header.Add("Authorization", "Bearer "+token)
		header.Add("X-Snowflake-Authorization-Token-Type", "KEYPAIR_JWT")
		header.Add("Content-Type", "application/json")
		header.Add("Accept", "application/json")

		assert.NoError(t, err)
		assert.Equal(t, header, req.Header)
		assert.Equal(t, snowflakeUrl+"?async=true", req.URL.String())
		assert.Equal(t, "POST", req.Method)
	})
	t.Run("build http request for getting a snowflake query status", func(t *testing.T) {
		req, err := buildRequest("GET", QueryInfo{}, account, token, queryId, false)

		assert.NoError(t, err)
		assert.Equal(t, snowflakeUrl+"/"+queryId, req.URL.String())
		assert.Equal(t, "GET", req.Method)
	})
	t.Run("build http request for deleting a snowflake query", func(t *testing.T) {
		req, err := buildRequest("POST", QueryInfo{}, account, token, queryId, true)

		assert.NoError(t, err)
		assert.Equal(t, snowflakeUrl+"/"+queryId+"/cancel", req.URL.String())
		assert.Equal(t, "POST", req.Method)
	})
}

func TestBuildResponse(t *testing.T) {
	t.Run("build http response", func(t *testing.T) {
		bodyStr := `{"data":{"statementHandle":"019c06a4-0000","message":"Statement executed successfully."}}`
		responseBody := io.NopCloser(strings.NewReader(bodyStr))
		response := &http.Response{Body: responseBody}
		actualData, err := buildResponse(response)
		assert.NoError(t, err)

		bodyByte, err := ioutil.ReadAll(strings.NewReader(bodyStr))
		assert.NoError(t, err)
		var expectedData map[string]interface{}
		err = json.Unmarshal(bodyByte, &expectedData)
		assert.NoError(t, err)
		assert.Equal(t, expectedData, actualData)
	})
}
