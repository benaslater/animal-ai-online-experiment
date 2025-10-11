import json
import boto3
import base64
import csv
import io
from datetime import datetime
from typing import Dict, Any, List, Tuple, Optional
import hashlib

s3_client = boto3.client('s3')
BUCKET_NAME: str = 'test-csv-file-storage'  # Replace with your bucket name
MAX_FILE_SIZE: int = 10 * 1024 * 1024  # 10MB limit

# Expected CSV header from your CSVWriter
EXPECTED_HEADER: List[str] = [
    "Episode", "Step", "Health", "Reward", 
    "XVelocity", "YVelocity", "ZVelocity",
    "XPosition", "YPosition", "ZPosition",
    "ActionForwardWithDescription", "ActionRotateWithDescription",
    "WasAgentFrozen?", "WasNotificationShown?",
    "WasRewardDispensed?", "DispensedRewardType", "CollectedRewardType",
    "WasSpawnerButtonTriggered?", "CombinedSpawnerInfo",
    "DataZoneMessage", "ActiveCamera", "CombinedRaycastData"
]


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Accepts CSV telemetry data, validates it, and uploads to S3
    
    Args:
        event: API Gateway event containing the request
        context: Lambda context object
        
    Returns:
        API Gateway response dictionary
    """
    
    try:      
        # Get CSV data (can be base64 encoded or plain text)
        csv_data: Optional[str] = event.get('csv_data')
        encoding: str = event.get('encoding', 'plain')  # 'plain' or 'base64'
        session_id: str = event.get('session_id', generate_session_id())
        user_id: str = event.get('user_id')
        
        if not csv_data:
            return error_response(400, f"Missing csv_data in request body. Got {event}")
        
        # Decode if base64
        if encoding == 'base64':
            try:
                csv_data = base64.b64decode(csv_data).decode('utf-8')
            except Exception as e:
                return error_response(400, f"Invalid base64 encoding: {str(e)}")
        
        # Validate file size
        if len(csv_data.encode('utf-8')) > MAX_FILE_SIZE:
            return error_response(413, f"File too large. Max size: {MAX_FILE_SIZE} bytes")
        
        # Validate CSV structure
        validation_result: ValidationResult = validate_csv(csv_data)
        if not validation_result['valid']:
            return error_response(400, f"Invalid CSV: {validation_result['error']}")
        
        # Generate S3 key with timestamp and session
        # TODO: Find the experiment ID for user_id and put the user in it
        s3_key: str = f"{user_id}/{session_id}.csv"
        
        # Upload to S3
        try:
            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=s3_key,
                Body=csv_data.encode('utf-8'),
                ContentType='text/csv',
                Metadata={
                    'session_id': session_id,
                    'row_count': str(validation_result['row_count']),
                    'upload_timestamp': datetime.utcnow().isoformat()
                }
            )
        except Exception as e:
            return error_response(500, f"S3 upload failed: {str(e)}")
        
        return {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Origin': '*',  # Restrict this to your GitHub Pages domain
                'Access-Control-Allow-Headers': 'Content-Type',
                'Content-Type': 'application/json'
            },
            'body': json.dumps({
                'message': 'Telemetry uploaded successfully',
                's3_key': s3_key,
                'row_count': validation_result['row_count'],
                'session_id': session_id
            })
        }
        
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        return error_response(500, "Internal server error")


class ValidationResult(Dict[str, Any]):
    """Type hint for validation result dictionary"""
    valid: bool
    error: Optional[str]
    row_count: Optional[int]


def validate_csv(csv_data: str) -> ValidationResult:
    """
    Validates that CSV matches the expected format from CSVWriter
    
    Args:
        csv_data: The CSV content as a string
        
    Returns:
        Dictionary with validation results containing:
            - valid: bool indicating if CSV is valid
            - error: str with error message if invalid
            - row_count: int number of data rows if valid
    """
    try:
        csv_file: io.StringIO = io.StringIO(csv_data)
        reader: csv.reader = csv.reader(csv_file)
        
        # Read header
        header: Optional[List[str]] = next(reader, None)
        if header is None:
            return {'valid': False, 'error': 'Empty CSV file', 'row_count': None}
        
        # Check header matches expected format
        if header != EXPECTED_HEADER:
            return {
                'valid': False, 
                'error': f'Header mismatch. Expected {len(EXPECTED_HEADER)} columns, got {len(header)}',
                'row_count': None
            }
        
        row_count: int = 0
        max_rows: int = 100000  # Prevent abuse with massive files
        
        for row_num, row in enumerate(reader, start=2):  # Start at 2 (header is row 1)
            row_count += 1
            
            if row_count > max_rows:
                return {
                    'valid': False, 
                    'error': f'Too many rows (max {max_rows})',
                    'row_count': None
                }
            
            # Validate row has correct number of columns
            if len(row) != len(EXPECTED_HEADER):
                if row[0].startswith('Positive Goals Collected'):
                    # Allow positive goals summary row
                    continue
                return {
                    'valid': False,
                    'error': f'Row {row_num}: Expected {len(EXPECTED_HEADER)} columns, got {len(row)}. ({row})',
                    'row_count': None
                }
            
            # Basic type validation for key numeric fields
            try:
                # Episode and Step should be integers
                int(row[0])  # Episode
                int(row[1])  # Step
                
                # Health and Reward should be floats
                float(row[2])  # Health
                float(row[3])  # Reward
                
                # Velocity and Position should be floats
                for i in range(4, 10):
                    float(row[i])
                    
            except ValueError:
                return {
                    'valid': False,
                    'error': f'Row {row_num}: Invalid numeric values in required fields',
                    'row_count': None
                }
        
        # Check for suspicious patterns
        if row_count == 0:
            return {
                'valid': False, 
                'error': 'No data rows found',
                'row_count': None
            }
        
        if row_count < 10 and row_count > 0:
            # Very small files might be test/spam, but we'll allow them
            pass
        
        return {
            'valid': True,
            'error': None,
            'row_count': row_count
        }
        
    except Exception as e:
        return {
            'valid': False, 
            'error': f'CSV parsing error: {str(e)}',
            'row_count': None
        }


def generate_session_id() -> str:
    """
    Generates a unique session ID based on timestamp
    
    Returns:
        12-character hexadecimal session ID
    """
    timestamp: str = datetime.utcnow().isoformat()
    return hashlib.md5(timestamp.encode()).hexdigest()[:12]


def error_response(status_code: int, message: str) -> Dict[str, Any]:
    """
    Returns a standardized error response
    
    Args:
        status_code: HTTP status code
        message: Error message to return
        
    Returns:
        API Gateway response dictionary
    """
    return {
        'statusCode': status_code,
        'headers': {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Content-Type': 'application/json'
        },
        'body': json.dumps({
            'error': message
        })
    }