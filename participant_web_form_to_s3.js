// --- CONFIGURATION ---
// Script properties are now retrieved inside functions
// to ensure they work properly with triggers
// ---------------------

/**
 * The main function triggered when a new form response is submitted.
 * @param {Object} e The event object passed by the form submission trigger.
 */
function onFormSubmit(e) {
  try {
    // Get script properties inside the function
    const scriptProperties = PropertiesService.getScriptProperties();
    const S3_BUCKET_NAME = scriptProperties.getProperty('S3_BUCKET_NAME');
    
    // 1. Get the data and headers
    const sheet = e.range.getSheet();
    const headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
    const newRowData = e.values;

    // 2. Determine the S3 Folder (Prefix) from the "What is your user id" question
    const userIdQuestion = "What is your user ID?";
    let userId = 'unknown_user'; // Default folder name
    
    // Find the column index for the User ID question
    const userIdIndex = headers.indexOf(userIdQuestion);

    if (userIdIndex > -1) {
      // Get the corresponding value from the new response row
      const rawUserId = newRowData[userIdIndex];
      // Sanitize the ID to be safe for a file path (e.g., remove spaces or special chars)
      userId = String(rawUserId).replace(/[^a-zA-Z0-9-]/g, '_').toLowerCase();
    }
    
    // 3. Format the data for S3 (as a simple CSV string)
    // Convert the raw timestamp string/value into a Date object, then get its time value
    const timestamp = new Date(newRowData[0]).getTime(); 
    const fileName = `response_${timestamp}.csv`;
    
    // The final S3 object key includes the user ID folder/prefix
    const s3Key = `${userId}/${fileName}`;
    
    // Create the CSV content: header row followed by the data row
    const csvContent = headers.join(',') + '\n' + newRowData.join(',');
    
    // Convert string content to a Blob
    const payloadBlob = Utilities.newBlob(csvContent, 'text/csv', fileName);

    // 4. Upload the file to S3
    uploadToS3(s3Key, payloadBlob, scriptProperties);

    Logger.log(`Successfully uploaded file: ${s3Key} to S3 bucket ${S3_BUCKET_NAME}.`);

  } catch (error) {
    Logger.log(`Error processing form submission: ${error.toString()}`);
    // Optional: Send an email alert on failure
    // MailApp.sendEmail('your-alert-email@example.com', 'S3 Upload Failed', 'Error: ' + error.toString());
  }
}

/**
 * Uploads a file (Blob) to a specified S3 bucket using AWS Signature Version 4.
 * @param {string} key The object key (file name/path) for S3.
 * @param {GoogleAppsScript.Base.Blob} blob The file content as a Blob.
 * @param {GoogleAppsScript.Properties.Properties} scriptProperties The script properties service.
 */
function uploadToS3(key, blob, scriptProperties) {
  // Retrieve AWS credentials inside the function
  const AWS_ACCESS_KEY_ID = scriptProperties.getProperty('AWS_ACCESS_KEY_ID');
  const AWS_SECRET_ACCESS_KEY = scriptProperties.getProperty('AWS_SECRET_ACCESS_KEY');
  const S3_BUCKET_NAME = scriptProperties.getProperty('S3_BUCKET_NAME');
  const AWS_REGION = scriptProperties.getProperty('AWS_REGION');
  
  // Add safety checks
  if (!AWS_SECRET_ACCESS_KEY) {
    throw new Error('AWS_SECRET_ACCESS_KEY is not set in Script Properties');
  }
  if (!AWS_ACCESS_KEY_ID) {
    throw new Error('AWS_ACCESS_KEY_ID is not set in Script Properties');
  }
  if (!S3_BUCKET_NAME) {
    throw new Error('S3_BUCKET_NAME is not set in Script Properties');
  }
  if (!AWS_REGION) {
    throw new Error('AWS_REGION is not set in Script Properties');
  }
  
  const service = 's3';
  const method = 'PUT';
  const contentType = blob.getContentType();
  const payload = blob.getBytes();
  
  // Generate date strings
  const date = new Date();
  const amzDate = Utilities.formatDate(date, "UTC", "yyyyMMdd'T'HHmmss'Z'");
  const dateStamp = Utilities.formatDate(date, "UTC", "yyyyMMdd");
  
  // Calculate content hash
  const payloadHash = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, payload)
    .map(function(byte) {
      return ('0' + (byte & 0xFF).toString(16)).slice(-2);
    }).join('');
  
  // Create canonical request
  const canonicalUri = '/' + key;
  const canonicalQuerystring = '';
  const canonicalHeaders = 'host:' + S3_BUCKET_NAME + '.s3.' + AWS_REGION + '.amazonaws.com\n' +
                          'x-amz-content-sha256:' + payloadHash + '\n' +
                          'x-amz-date:' + amzDate + '\n';
  const signedHeaders = 'host;x-amz-content-sha256;x-amz-date';
  
  const canonicalRequest = method + '\n' +
                          canonicalUri + '\n' +
                          canonicalQuerystring + '\n' +
                          canonicalHeaders + '\n' +
                          signedHeaders + '\n' +
                          payloadHash;
  
  // Create string to sign
  const algorithm = 'AWS4-HMAC-SHA256';
  const credentialScope = dateStamp + '/' + AWS_REGION + '/' + service + '/aws4_request';
  
  const canonicalRequestHash = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, canonicalRequest)
    .map(function(byte) {
      return ('0' + (byte & 0xFF).toString(16)).slice(-2);
    }).join('');
  
  const stringToSign = algorithm + '\n' +
                      amzDate + '\n' +
                      credentialScope + '\n' +
                      canonicalRequestHash;
  
  // Calculate signature using a working approach for Google Apps Script
  // We need to convert byte arrays to hex strings as intermediate keys
  function sign(key, message) {
    if (typeof key === 'string') {
      return Utilities.computeHmacSignature(Utilities.MacAlgorithm.HMAC_SHA_256, message, key);
    }
    // Convert byte array to hex string to use as key
    let hexKey = '';
    for (let i = 0; i < key.length; i++) {
      const byte = key[i] < 0 ? key[i] + 256 : key[i];
      hexKey += String.fromCharCode(byte);
    }
    return Utilities.computeHmacSha256Signature(message, hexKey);
  }
  
  function hmacSha256(key, data) {
    let binaryKey;
    if (typeof key === 'string') {
      binaryKey = Utilities.newBlob(key).getBytes();
    } else {
      binaryKey = key;
    }
    
    let binaryData = Utilities.newBlob(data).getBytes();
    
    // Perform HMAC-SHA256 manually since Google Apps Script has issues with binary keys
    const blockSize = 64;
    
    // If key is longer than block size, hash it
    if (binaryKey.length > blockSize) {
      binaryKey = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, binaryKey);
    }
    
    // Pad key to block size
    const paddedKey = [];
    for (let i = 0; i < blockSize; i++) {
      paddedKey[i] = i < binaryKey.length ? binaryKey[i] : 0;
    }
    
    // Create inner and outer padded keys
    const innerPadded = paddedKey.map(b => (b & 0xFF) ^ 0x36);
    const outerPadded = paddedKey.map(b => (b & 0xFF) ^ 0x5C);
    
    // Inner hash
    const innerData = innerPadded.concat(Array.from(binaryData));
    const innerHash = Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, innerData);
    
    // Outer hash
    const outerData = outerPadded.concat(Array.from(innerHash));
    return Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, outerData);
  }
  
  const kDate = hmacSha256('AWS4' + AWS_SECRET_ACCESS_KEY, dateStamp);
  const kRegion = hmacSha256(kDate, AWS_REGION);
  const kService = hmacSha256(kRegion, service);
  const kSigning = hmacSha256(kService, 'aws4_request');
  
  const signatureBytes = hmacSha256(kSigning, stringToSign);
  const signature = signatureBytes.map(function(byte) {
    const b = byte < 0 ? byte + 256 : byte;
    return ('0' + b.toString(16)).slice(-2);
  }).join('');
  
  // Create authorization header
  const authorizationHeader = algorithm + ' ' +
                             'Credential=' + AWS_ACCESS_KEY_ID + '/' + credentialScope + ', ' +
                             'SignedHeaders=' + signedHeaders + ', ' +
                             'Signature=' + signature;
  
  // S3 URL for the upload
  const url = 'https://' + S3_BUCKET_NAME + '.s3.' + AWS_REGION + '.amazonaws.com/' + key;
  
  // Make the PUT request to S3
  const options = {
    method: 'put',
    headers: {
      'Authorization': authorizationHeader,
      'Content-Type': contentType,
      'x-amz-content-sha256': payloadHash,
      'x-amz-date': amzDate
    },
    payload: payload,
    muteHttpExceptions: true
  };
  
  const response = UrlFetchApp.fetch(url, options);
  
  if (response.getResponseCode() !== 200) {
    throw new Error('S3 Upload Failed: HTTP ' + response.getResponseCode() + '. Response: ' + response.getContentText());
  }
}