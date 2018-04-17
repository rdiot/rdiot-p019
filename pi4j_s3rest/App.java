package com.rdiot.pi4j_s3rest;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.rdiot.pi4j.dht11;
import com.rdiot.s3rest.auth.AWS4SignerBase;
import com.rdiot.s3rest.auth.AWS4SignerForAuthorizationHeader;
import com.rdiot.s3rest.util.BinaryUtils;
import com.rdiot.s3rest.util.HttpUtils;

public class App 
{
    /** Put your access key here **/
    private static final String awsAccessKey = "input";
    
    /** Put your secret key here **/
    private static final String awsSecretKey = "input";
    
    /** Put your bucket name here **/
    private static final String bucketName = "rdkim-test";
    
    /** The name of the region where the bucket is created. (e.g. us-west-1) **/
    private static final String regionName = "ap-northeast-2";
    
	public static String objectContent;
	
    public static void main( String[] args ) throws JsonProcessingException
    {
        System.out.println("#################################################################");
        System.out.println( "RDIoT Project P019 - Pi4J + Amazon S3 Rest API + Amazon Athena" );
        System.out.println("#################################################################");
        
    	dht11 dht = new dht11();
    	
        for (int i=0; i<10; i++) {
        	
            try {
				Thread.sleep(2000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
            
            objectContent = dht.getTemperature();
            
            if(objectContent != null) {
        		System.out.println(objectContent);
        		putS3Object(bucketName, regionName, awsAccessKey, awsSecretKey);
            	break;
            }            
         }     
    }
    
    /**
     * Uploads content to an Amazon S3 object in a single call using Signature V4 authorization.
     */
    public static void putS3Object(String bucketName, String regionName, String awsAccessKey, String awsSecretKey) {
        System.out.println("************************************************");
        System.out.println("*      Put Sensor Data(DHT11) to Amazon S3     *");
        System.out.println("************************************************");
        
        URL endpointUrl;
        
        GregorianCalendar calendar = new GregorianCalendar();
        int year = calendar.get(Calendar.YEAR);
        int month = calendar.get(Calendar.MONTH)+1;
        int date = calendar.get(Calendar.DATE);
        int hour = calendar.get(Calendar.HOUR);
        int min = calendar.get(Calendar.MINUTE);
        int sec = calendar.get(Calendar.SECOND);
        
        try {
            if (regionName.equals("us-east-1")) {
                endpointUrl = new URL("https://s3.amazonaws.com/" + bucketName + "/pi_dht11_"+year+month+date+hour+min+sec+".json");
            } else {
                endpointUrl = new URL("https://s3-" + regionName + ".amazonaws.com/" + bucketName + "/pi_dht11_"+year+month+date+hour+min+sec+".json");
            }
        } catch (MalformedURLException e) {
            throw new RuntimeException("Unable to parse service endpoint: " + e.getMessage());
        }
        
        // precompute hash of the body content
        byte[] contentHash = AWS4SignerBase.hash(objectContent);
        String contentHashString = BinaryUtils.toHex(contentHash);
        
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("x-amz-content-sha256", contentHashString);
        headers.put("content-length", "" + objectContent.length());
        headers.put("x-amz-storage-class", "REDUCED_REDUNDANCY");
        
        AWS4SignerForAuthorizationHeader signer = new AWS4SignerForAuthorizationHeader(
                endpointUrl, "PUT", "s3", regionName);
        String authorization = signer.computeSignature(headers, 
                                                       null, // no query parameters
                                                       contentHashString, 
                                                       awsAccessKey, 
                                                       awsSecretKey);
                
        // express authorization for this as a header
        headers.put("Authorization", authorization);
        
        // PUT
        String response = HttpUtils.invokeHttpRequest(endpointUrl, "PUT", headers, objectContent);
        System.out.println("--------- Response content ---------");
        System.out.println(response);
        System.out.println("------------------------------------");
 
    }
    
}
