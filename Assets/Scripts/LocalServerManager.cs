using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using UnityEngine;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;

public class LocalServerManager : MonoBehaviour
{
    private AmazonDynamoDBClient _dynamoDBClient;

    private string _tableName;
    private string _studentID;
    private string _score;
    private string _time;
    private string _newTime;
    private string _attributeName;
    private string _comparisonOperator;
    private string _attributeValue;

    private void Start()
    {
        // Set up the AWS SDK configuration
        var config = new AmazonDynamoDBConfig
        {
            ServiceURL = "http://localhost:8000", // URL of your DynamoDB Local instance
        };

        // Instantiate the AmazonDynamoDBClient with the specified configuration
        _dynamoDBClient = new AmazonDynamoDBClient(config);
    }

    public void InputTableName(string inputName)
    {
        _tableName = inputName;
    }
    
    public void InputStudentID(string inputID)
    {
        _studentID = inputID;
    }
    
    public void InputScore(string inputScore)
    {
        _score = inputScore;
    }

    public void InputTime(string inputTime)
    {
        _time = inputTime;
    }

    public void InputNewTime(string inputNewTime)
    {
        _newTime = inputNewTime;
    }

    public void InputAttributeName(string inputAttributeName)
    {
        _attributeName = inputAttributeName;
    }
    
    public void InputComparisonOperator(string inputComparisonOperator)
    {
        _comparisonOperator = inputComparisonOperator;
    }
    
    public void InputAttributeValue(string inputAttributeValue)
    {
        _attributeValue = inputAttributeValue;
    }
    
    public void PressCreateTable()
    {
        StartCoroutine(CreateTable(_tableName));
    }

    public void PressDeleteTable()
    {
        StartCoroutine(DeleteTable(_tableName));
    }

    public void PressListTable()
    {
        ListTable();
    }

    public void PressPutItem()
    {
        StartCoroutine(PutItem(_studentID, _score, _time, _tableName));
    }

    public void PressScanAllItem()
    {
        StartCoroutine(ScanAllItemInTable(_tableName));
    }
    
    public void PressScanItem()
    {
        StartCoroutine(ScanItemInTable(_tableName));
    }

    public async void PressGetItem()
    {
        var item = await GetItemAsync(_studentID, _score, _tableName);
        if (item != null && item.Count > 0)
        {
            // Access the item attributes here
            var studentID = item["StudentID"].S;
            var score = item["Score"].N;
            var time = item["Time"].N;
            Debug.Log($"StudentID: {studentID} Score: {score} Time: {time}");
        }
        else
        {
            // Handle the case where no item was found or the retrieved item has no attributes
            Debug.Log("No item/attribute found!");
        }
    }
    
    public async void PressDeleteItem()
    {
        var item = await GetItemAsync(_studentID, _score, _tableName);
        if (item != null && item.Count > 0)
        {
            // The item exists in the table, so we can delete it
            StartCoroutine(DeleteItem(_studentID, _score, _tableName));
        }
        else
        {
            // The item does not exist in the table, so we don't need to delete it
            Debug.Log("No item/attribute found!");
        }
    }

    public async void PressUpdateItem()
    {
        var item = await GetItemAsync(_studentID, _score, _tableName);
        if (item != null && item.Count > 0)
        {
            // Item exists, perform update
            StartCoroutine(UpdateItem(_studentID, _score, _newTime, _tableName));
        }
        else
        {
            // The item does not exist in the table, so we don't need to update it
            Debug.Log("No item/attribute found!");
        }
    }

    private IEnumerator DeleteTable(string tableName)
    {
        var request = new DeleteTableRequest
        {
            TableName = tableName
        };

        var response = _dynamoDBClient.DeleteTableAsync(request);
        yield return new WaitUntil(() => response.IsCompleted);

        if (response.Exception == null)
        {
            Debug.Log("Table deleted successfully!");
        }
        else
        {
            Debug.LogError($"Failed to delete table: {response.Exception}");
        }
    }

    private IEnumerator CreateTable(string tableName)
    {
        // Define the table schema
        var request = new CreateTableRequest
        {
            TableName = tableName,
            AttributeDefinitions = new List<AttributeDefinition>
            {
                new AttributeDefinition
                {
                    AttributeName = "StudentID",
                    AttributeType = ScalarAttributeType.S //string
                },
                new AttributeDefinition
                {
                    AttributeName = "Score",
                    AttributeType = ScalarAttributeType.N //number
                }
            },
            KeySchema = new List<KeySchemaElement>
            {
                new KeySchemaElement
                {
                    AttributeName = "StudentID",
                    KeyType = KeyType.HASH //partition key
                },
                new KeySchemaElement
                {
                    AttributeName = "Score",
                    KeyType = KeyType.RANGE //sort key
                }
            },
            ProvisionedThroughput = new ProvisionedThroughput
            {
                ReadCapacityUnits = 5,
                WriteCapacityUnits = 5
            }
        };

        // Send the CreateTable request to DynamoDB local
        var response = _dynamoDBClient.CreateTableAsync(request);
        yield return new WaitUntil(() => response.IsCompleted);

        if (response.Exception == null)
        {
            Debug.Log("Table created successfully!");
        }
        else
        {
            Debug.LogError($"Failed to create table: {response.Exception}");
        }
    }
    
    private async void ListTable()
    {
        string lastEvaluatedTableName = null;
        do
        {
            var request = new ListTablesRequest
            {
                Limit = 10, // Page size.
                ExclusiveStartTableName = lastEvaluatedTableName
            };

            var response = await _dynamoDBClient.ListTablesAsync(request);
            foreach (var tableName in response.TableNames)
            {
                Debug.Log(tableName);
            }

            if (response.TableNames.Count == 0)
            {
                Debug.Log("No table created!");
            }

            lastEvaluatedTableName = response.LastEvaluatedTableName;

        } while (lastEvaluatedTableName != null);
    }

    private IEnumerator PutItem(string studentID, string score, string time, string tableName)
    {
        var item = new Dictionary<string, AttributeValue>
        {
            ["StudentID"] = new AttributeValue { S = studentID },
            ["Score"] = new AttributeValue { N = score },
            ["Time"] = string.IsNullOrEmpty(time) ? new AttributeValue { N = "0" } : new AttributeValue { N = time }
        };

        var request = new PutItemRequest
        {
            TableName = tableName,
            Item = item,
        };

        var response =  _dynamoDBClient.PutItemAsync(request);
        yield return new WaitUntil(() => response.IsCompleted);
        
        if (response.Exception == null)
        {
            Debug.Log("Put item successfully!");
        }
        else
        {
            Debug.LogError($"Failed to put item: {response.Exception}");
        }
    }
    
    private async Task<Dictionary<string, AttributeValue>> GetItemAsync(string studentID, string score, string tableName)
    {
        var key = new Dictionary<string, AttributeValue>
        {
            ["StudentID"] = new AttributeValue { S = studentID },
            ["Score"] = new AttributeValue { N = score },
        };

        var request = new GetItemRequest
        {
            Key = key,
            TableName = tableName,
        };

        var response = await _dynamoDBClient.GetItemAsync(request);
        return response.Item;
    }
    
    private IEnumerator DeleteItem(string studentID, string score, string tableName)
    {
        var key = new Dictionary<string, AttributeValue>
        {
            ["StudentID"] = new AttributeValue { S = studentID },
            ["Score"] = new AttributeValue { N = score },
        };

        var request = new DeleteItemRequest
        {
            TableName = tableName,
            Key = key,
        };

        var response = _dynamoDBClient.DeleteItemAsync(request);
        yield return new WaitUntil(() => response.IsCompleted);
        
        if (response.Exception == null)
        {
            Debug.Log("Delete item successfully!");
        }
        else
        {
            Debug.LogError($"Failed to delete item: {response.Exception}");
        }
    }

    private IEnumerator UpdateItem(string studentID, string score, string newTime, string tableName)
    {
        var key = new Dictionary<string, AttributeValue>
        {
            ["StudentID"] = new AttributeValue { S = studentID },
            ["Score"] = new AttributeValue { N = score },
        };
        
        var updates = new Dictionary<string, AttributeValueUpdate>
        {
            ["Time"] = new AttributeValueUpdate
            {
                Action = AttributeAction.PUT,
                Value = new AttributeValue { N = newTime },
            }
        };
        
        var request = new UpdateItemRequest
        {
            AttributeUpdates = updates,
            Key = key,
            TableName = tableName,
        };
        
        var response = _dynamoDBClient.UpdateItemAsync(request);
        yield return new WaitUntil(() => response.IsCompleted);
        
        if (response.Exception == null)
        {
            Debug.Log("Update item successfully!");
        }
        else
        {
            Debug.LogError($"Failed to update item: {response.Exception}");
        }
    }
    
    private IEnumerator ScanAllItemInTable(string tableName)
    {
        var request = new ScanRequest
        {
            TableName = tableName
        };
        
        var response = _dynamoDBClient.ScanAsync(request);
        yield return new WaitUntil(() => response.IsCompleted);
        
        if (response.Exception == null)
        {
            Debug.Log("Scan all item successfully!");
            if (response.Result.Items.Any())
            {
                foreach (var item in response.Result.Items)
                {
                    // Access the attributes of each item in the response
                    var studentID = item["StudentID"].S;
                    var score = item["Score"].N;
                    var time = item["Time"].N;
                    Debug.Log($"StudentID: {studentID} Score: {score} Time: {time}");
                }
            }
            else
            {
                // Handle the case where there are no items to process
                Debug.Log("No items found in the DynamoDB table.");
            }
        }
        else
        {
            Debug.LogError($"Error scanning table: {response.Exception}");
        }
    }
    
    private IEnumerator ScanItemInTable(string tableName)
    {
        var request = new ScanRequest
        {
            TableName = tableName,
            ExpressionAttributeNames = new Dictionary<string, string>
            {
                { "#attributeName", _attributeName },
            },
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                { ":val", new AttributeValue { N = _attributeValue } },
            },
            FilterExpression = "#attributeName" + _comparisonOperator + ":val"
        };
        
        var response = _dynamoDBClient.ScanAsync(request);
        yield return new WaitUntil(() => response.IsCompleted);
        
        if (response.Exception == null)
        {
            Debug.Log("Scan all item successfully!");
            if (response.Result.Items.Any())
            {
                foreach (var item in response.Result.Items)
                {
                    // Access the attributes of each item in the response
                    var studentID = item["StudentID"].S;
                    var score = item["Score"].N;
                    var time = item["Time"].N;
                    Debug.Log($"StudentID: {studentID} Score: {score} Time: {time}");
                }
            }
            else
            {
                // Handle the case where there are no items to process
                Debug.Log("No items found in the DynamoDB table.");
            }
        }
        else
        {
            Debug.LogError($"Error scanning table: {response.Exception}");
        }
    }
}
