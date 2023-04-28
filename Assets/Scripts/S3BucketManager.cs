using System.IO;
using Amazon.S3;
using Amazon.S3.Model;
using UnityEngine;

public class S3BucketManager : MonoBehaviour
{
    private AuthenticationManager _authenticationManager;
    
    private string _bucketName;
    private string _objectKey;

    void Awake()
    {
        _authenticationManager = FindObjectOfType<AuthenticationManager>();
    }
    
    public void InputBucketName(string inputBucketName)
    {
        _bucketName = inputBucketName;
    }
    
    public void InputObjectKey(string inputObjectKey)
    {
        _objectKey = inputObjectKey;
    }
    
    public void PressGetS3()
    {
        if (_authenticationManager == null)
        {
            Debug.Log("Not support in local, please log in first!");
        }
        else
        {
            ExecuteS3();
        }
    }
    
    private async void ExecuteS3()
    {
        Debug.Log("ExecuteS3");
        
        var s3Client = new AmazonS3Client(_authenticationManager.GetCredentials(), AuthenticationManager.Region);
        
        var request = new GetObjectRequest
        {
            BucketName = _bucketName,
            Key = _objectKey
        };
        
        try
        {
            var response = await s3Client.GetObjectAsync(request);
            using (var reader = new StreamReader(response.ResponseStream))
            {
                var contents = await reader.ReadToEndAsync();
                Debug.Log("S3 Object Contents: " + contents);
            }
        }
        catch (AmazonS3Exception e)
        {
            Debug.LogError("Error encountered when retrieving object: " + e.Message);
        }
    }
}
