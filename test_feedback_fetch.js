// Test script for the feedback upload lambda fetch (reproduces submitFeedback from completion.html:303)
// Usage: node test_feedback_fetch.js

const FEEDBACK_UPLOAD_LAMBDA_URL = 'https://x314h4yzq5.execute-api.eu-north-1.amazonaws.com/Prod/telemetry';

const experimentId = 'test_2504_1118';
const participantId = 'be6d381c1dc9cbb7';
const feedbackText = 'This is a test feedback message.';

const payload = {
  file_data: feedbackText,
  file_type: 'txt',
  experiment_id: experimentId,
  user_id: participantId,
};

async function testFetch() {
  console.log('URL:     ', FEEDBACK_UPLOAD_LAMBDA_URL);
  console.log('Payload: ', JSON.stringify(payload));
  console.log('---');

  let response;
  try {
    response = await fetch(FEEDBACK_UPLOAD_LAMBDA_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
  } catch (e) {
    console.error('FETCH FAILED (network/CORS/DNS error):', e.message);
    return;
  }

  console.log('HTTP status:', response.status, response.statusText);
  console.log('Headers:');
  for (const [k, v] of response.headers) {
    console.log(`  ${k}: ${v}`);
  }

  const text = await response.text();
  console.log('Raw body:', text);

  if (!response.ok) {
    console.error('→ response.ok is false — submitFeedback would show "Submission failed"');
    return;
  }

  let outerJson;
  try {
    outerJson = JSON.parse(text);
  } catch (e) {
    console.error('→ Failed to parse outer JSON:', e.message);
    return;
  }

  console.log('Outer JSON:', JSON.stringify(outerJson, null, 2));

  if (outerJson.statusCode !== 200) {
    console.error('→ outerJson.statusCode is not 200 — submitFeedback would show "Submission failed"');
    return;
  }

  console.log('→ Success: feedback upload accepted');
}

testFetch();
