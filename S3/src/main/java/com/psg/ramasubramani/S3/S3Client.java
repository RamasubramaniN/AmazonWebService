package com.psg.ramasubramani.S3;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map.Entry;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.iterable.S3Versions;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.BucketVersioningConfiguration;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.GetBucketLocationRequest;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GroupGrantee;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Permission;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.SetBucketVersioningConfigurationRequest;
import com.amazonaws.services.s3.model.VersionListing;

/**
 * rn5
 *
 */
public class S3Client {
	
	private AmazonS3 amazonS3;
	
	public S3Client() {
		amazonS3 = getS3Client();
	}

	/**
	 * A bucket is a container for objects stored in Amazon S3. Every object is
	 * contained in a bucket. For example, if the object named photos/puppy.jpg is
	 * stored in the johnsmith bucket, then it is addressable using the URL
	 * http://johnsmith.s3.amazonaws.com/photos/puppy.jpg Buckets serve several
	 * purposes: they organize the Amazon S3 namespace at the highest level, they
	 * identify the account responsible for storage and data transfer charges, they
	 * play a role in access control, and they serve as the unit of aggregation for
	 * usage reporting.
	 */
	public void createBucket(String bucketName) {
		if (!amazonS3.doesBucketExist(bucketName)) {
			amazonS3.createBucket(bucketName);
			System.out.println("Created bucket. Bucket Name : " + bucketName);
		} else {
			System.out.println("Bucket with the " + bucketName + " exists.");
		}
	}

	public void deleteBuckets(String bucketName) {
		if (amazonS3.doesBucketExist(bucketName)) {
			amazonS3.deleteBucket(bucketName);
			System.out.println("Deleted bucket. Bucket Name : " + bucketName);
		} else {
			System.out.println("No bucket with the name " + bucketName);
		}
	}

	public void listBuckets() {
		for (Bucket bucket : amazonS3.listBuckets()) {
			String bucketLocation = amazonS3.getBucketLocation(new GetBucketLocationRequest(bucket.getName()));
			System.out.println(bucketLocation);// us-west-2
			System.out.println("Bucket Name : " + bucket.getName());// ramasubramani
			System.out.println("Bucket creation time : " + bucket.getCreationDate());// Sun Apr 15 11:51:06 IST 2018
			System.out.println("Bucket Owner : " +  bucket.getOwner());// S3Owner [name=r-ns-learning-account-16483,id=a9dcc0eaa7159446da3efc472bc2171fe5bf19b0e4695b1e12236adb2dfcb541]
		}
	}

	public AmazonS3 getS3Client() {
		AWSCredentials awsCredentials = new BasicAWSCredentials("*****",
				"*****");
		AmazonS3 amazonS3 = AmazonS3ClientBuilder.standard()
				.withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
				//.withRegion(Regions.US_EAST_1)
				.build();
		return amazonS3;
	}

	/**
	 * Objects are the fundamental entities stored in Amazon S3. Objects consist of
	 * object data and metadata. The data portion is opaque to Amazon S3. The
	 * metadata is a set of name-value pairs that describe the object. These include
	 * some default metadata, such as the date last modified, and standard HTTP
	 * metadata, such as Content-Type. You can also specify custom metadata at the
	 * time the object is stored.
	 */
	public void createObjects(String bucketName, String keyName) {
		try {
			//add custom meta data to the objects. User defined meta data should be prefixed by "x-amz-meta-"
			ObjectMetadata objectMetadata = new ObjectMetadata();
			objectMetadata.addUserMetadata("fileType", "properties");
			objectMetadata.addUserMetadata("x-amz-meta-totalProperties", "5");
			
			File file = new File("/Users/rn5/Desktop/bootstrap.properties");
			//Actual Url : https://s3-us-west-2.amazonaws.com/ramasubramani/MyPhoto

			amazonS3.putObject(new PutObjectRequest(bucketName, keyName, file)).setMetadata(objectMetadata);
			System.out.println("Object created. Bucket : " + bucketName + ". Key : " + keyName);

		} catch (AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException, which " + "means your request made it "
					+ "to Amazon S3, but was rejected with an error response" + " for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException, which " + "means the client encountered "
					+ "an internal error while trying to " + "communicate with S3, "
					+ "such as not being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		}
	}
	
	/** If there are objects in the buckets, you will not be able to delete a bucket. So, clear all objects
	 * before attempting to delete bucket.
	 */
	public void emptyObjects(String bucketName) {
		for (Bucket bucket : amazonS3.listBuckets()) {
			if (bucket.getName().equals(bucketName)) {
				ObjectListing objectListing = amazonS3.listObjects(bucketName);
				for(S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
					System.out.println("Object Key : " + objectSummary.getKey());
					amazonS3.deleteObject(bucketName, objectSummary.getKey());
					System.out.println("Object deleted. Key : " + objectSummary.getKey());
				}
			}
		}
		System.out.println("Deleted all Objects in the bucket. Bucket Name : " + bucketName);
	}
	
	public void getObject(String bucketName, String keyName) throws IOException {
		S3Object s3Object = amazonS3.getObject(bucketName, keyName);
		File file = new File("/Users/rn5/Personal/S3/bootstarp.txt");
		InputStream inputStream = s3Object.getObjectContent();
		byte[] buffer = new byte[10000];
		inputStream.read(buffer);
		FileOutputStream fileOutputStream = new FileOutputStream(file);
		fileOutputStream.write(buffer);
		inputStream.close();
		fileOutputStream.close();
		System.out.println("Retreived Object. Bucket : " + bucketName + ". Key : " + keyName);

	}
	
	public void enableAccessControl(String bucketName, String keyName) {
		AccessControlList accessControlList = new AccessControlList();
		accessControlList.grantPermission(GroupGrantee.AllUsers, Permission.Read);
		File file = new File("/Users/rn5/Desktop/spcf-meta-conf.xml");
		//acl.grantPermission(new EmailAddressGrantee("user@email.com"), Permission.WriteAcp);
		amazonS3.putObject(new PutObjectRequest(bucketName, keyName, file).withAccessControlList(accessControlList));
	}
	
	public void listObjectsUsingListObjectsRequest(String bucketName) {
		// Listing object alternate way.
		System.out.println("Using ListObjectsRequest to list objects");
		ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(bucketName);
		ObjectListing objectListing = amazonS3.listObjects(listObjectsRequest);
		for (S3ObjectSummary s3ObjectSummary : objectListing.getObjectSummaries()) {
			System.out.println("Key " + s3ObjectSummary.getKey() + " Size " + s3ObjectSummary.getSize());
		}
	}

	public void copyBucket(String source, String target) {
		System.out.println("Copying objects from bucket " + source + ", target " + target);
		ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(source);
		ObjectListing objectListing = amazonS3.listObjects(listObjectsRequest);
		for (S3ObjectSummary s3ObjectSummary : objectListing.getObjectSummaries()) {
			System.out.println("Key " + s3ObjectSummary.getKey() + " Size " + s3ObjectSummary.getSize());
			CopyObjectRequest copyObjectRequest = new CopyObjectRequest(source, s3ObjectSummary.getKey(), target,
					s3ObjectSummary.getKey());
			amazonS3.copyObject(copyObjectRequest);
		}
		System.out.println("Copied objects from bucket " + source + ", target " + target);
	}
	
	public void printObjectMetaData(String bucket, String key) {
		GetObjectMetadataRequest getObjectMetadataRequest = new GetObjectMetadataRequest(bucket, key);
		ObjectMetadata objectMetadata = amazonS3.getObjectMetadata(getObjectMetadataRequest);
		System.out.println("***** Raw Meta data of the object. bucket : " + bucket + " key : " + key + " *****");
		for(Entry<String, Object> entry : objectMetadata.getRawMetadata().entrySet()) {
			System.out.println("Metadata. Name : " + entry.getKey() + " Value : " + entry.getValue());
		}
		System.out.println("*******************************************************");
	}
	
	public void enableVersioningForBucket(String bucketName) {
		BucketVersioningConfiguration bucketVersioningConfiguration = new 
				BucketVersioningConfiguration().withStatus("Enabled");
		SetBucketVersioningConfigurationRequest setBucketVersioningConfigurationRequest = 
				new SetBucketVersioningConfigurationRequest(bucketName, bucketVersioningConfiguration);
		amazonS3.setBucketVersioningConfiguration(setBucketVersioningConfigurationRequest);
		BucketVersioningConfiguration configuredConfiguration = amazonS3.getBucketVersioningConfiguration(bucketName);
		System.out.println(configuredConfiguration.getStatus());
	}
	
	public void listAllVersions(String bucketName) {
		ListVersionsRequest listVersionsRequest = new ListVersionsRequest().withBucketName(bucketName).withMaxResults(5);
		VersionListing versionListing = amazonS3.listVersions(listVersionsRequest);
		for(S3VersionSummary objectSummary : versionListing.getVersionSummaries()) {
			System.out.println("Key : " + objectSummary.getKey() + " , Version : " + objectSummary.getVersionId());
		}
	}
	
	public void deleteAllVersions(String bucketName) {
		for ( S3VersionSummary version : S3Versions.inBucket(amazonS3, bucketName) ) {
		    String key = version.getKey();
		    String versionId = version.getVersionId();          
		    amazonS3.deleteVersion(bucketName, key, versionId);
		}
	}
}
