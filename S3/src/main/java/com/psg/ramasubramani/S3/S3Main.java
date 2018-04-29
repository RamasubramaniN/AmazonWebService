package com.psg.ramasubramani.S3;

import java.io.IOException;

/**
 * @author rn5
 * There are two kinds of metadata: 1) system metadata 2) user-defined metadata.
 * 1) System-Defined Metadata
 * For each object stored in a bucket, Amazon S3 maintains a set of system metadata. Amazon S3 processes 
 * this system metadata as needed. For example, Amazon S3 maintains object creation date and size metadata 
 * and uses this information as part of object management.
 * a) Metadata such as object creation date is system controlled where only Amazon S3 can modify the value.
 * b) Other system metadata, such as the storage class configured for the object and whether the object has 
 * server-side encryption enabled, are examples of system metadata whose values you control.
 *
 */
public class S3Main {
	public static void main(String[] args) throws IOException {
		String bucket1 = "ramasubramani";
		String bucket2 = "subramaniam";
		String key1 = "Files/TextFiles/file1";//Buckets cannot have / symbol. 
		//To create directories structure use keyname. So ramasubramani/Files/TextFiles is a directory
		//File name is file1.
		String key2 = "file2";
		S3Client s3Client = new S3Client();
		s3Client.createObjects(bucket1, key1);
		s3Client.listAllVersions(bucket1); 
		//URL for each version
		//https://s3-us-west-2.amazonaws.com/ramasubramani/file2?versionId=jRcreCl25upVoKeyXMbknIx0sdISABPO
		//https://s3-us-west-2.amazonaws.com/ramasubramani/file2?versionId=mdLuIO8r6N5xoGgsxyWbdeCMpELqWB_u
		//(2 versions of the objects)
		//Key : Files/TextFiles/file1 , Version : JFuIBfbSDP0uJPt7zE3YpUVdfwIwWY.l
		//Key : Files/TextFiles/file1 , Version : QDHM4FZKMSXHuuxdWbWZGoV6o4ukywCi 

		s3Client.emptyObjects(bucket1);
		s3Client.emptyObjects(bucket2);
		s3Client.deleteAllVersions(bucket1);
		
		s3Client.deleteBuckets(bucket2);
		s3Client.deleteBuckets(bucket1);
		//DELETE https://subramani.s3-us-west-2.amazonaws.com / 
		//Headers: (x-amz-content-sha256: UNSIGNED-PAYLOAD, Authorization: AWS4-HMAC-SHA256 
		//Credential=AKIAILZXTUGAPIRFT4JQ/20180418/us-west-2/s3/aws4_request, 
		//SignedHeaders=amz-sdk-invocation-id;amz-sdk-retry;content-type;host;user-agent;x-amz-content-sha256;x-amz-date, Signature=4616f59f2b8c9076a26a23458e759d8c3d630f89f39bd61696959afcdf658fe8, X-Amz-Date: 20180418T132844Z, User-Agent: aws-sdk-java/1.11.163 Mac_OS_X/10.13.3 Java_HotSpot(TM)_64-Bit_Server_VM/25.101-b13/1.8.0_101, amz-sdk-invocation-id: 1f70b1d6-4614-bc61-0606-e469637145b6, Host: subramani.s3-us-west-2.amazonaws.com, amz-sdk-retry: 0/0/500, Content-Type: application/octet-stream, ) 
		
		//Internet is down. Caused by: java.net.UnknownHostException: ramasubramani.s3-us-west-2.amazonaws.com
		//s3Client.deleteBuckets(bucketName);//Exception in thread "main" com.amazonaws.services.s3.model.AmazonS3Exception: 
		//The bucket you tried to delete is not empty (Service: Amazon S3; Status Code: 409; Error Code: BucketNotEmpty; 
		//Request ID: 15E4334D768962EE), S3 Extended Request ID: 1YS9yaiol9hGXPLDJlQr8GoGdBSaAp4IhUUL4eoabCeFOUixTbo8o1Iq32277ckE/lsLnEmKai8=
	
		s3Client.createBucket(bucket1);
		s3Client.enableVersioningForBucket(bucket1);
		s3Client.listAllVersions(bucket1);
		s3Client.createBucket(bucket2);
		
		s3Client.listBuckets();
		
		s3Client.createObjects(bucket1, key1);
		s3Client.getObject(bucket1, key1);
		s3Client.printObjectMetaData(bucket1, key1);
		
		s3Client.enableAccessControl(bucket1, key2);
		
		s3Client.copyBucket(bucket1, bucket2);
		s3Client.listObjectsUsingListObjectsRequest(bucket2);
		
		//If bucket does not exist
		//Exception in thread "main" com.amazonaws.services.s3.model.AmazonS3Exception: 
		//The specified bucket does not exist (Service: Amazon S3; Status Code: 404; 
		//Error Code: NoSuchBucket; Request ID: 79A0971B3A986C2B), 
		//S3 Extended Request ID: /Wp1uJJRxy6ddj1+xvrMhnEEfQWlpJSS8wUW08qM3a07P/hIiNzHfCNlRTazZmtQCSa4kwgNW+Q=

	}
}
