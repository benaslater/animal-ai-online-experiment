// Test script for the lifecycle lambda fetch (reproduces the call from index.html:250)
// Usage: node test_fetch.js

const LIFECYCLE_LAMBDA_URL = 'https://ovqx9h40zg.execute-api.eu-north-1.amazonaws.com/test/telemetry';

const experimentId = 'test_2504_1118';
const userId = 'f2b1ad2ab8a1d2cd';

async function testFetch() {
  console.log('URL:     ', LIFECYCLE_LAMBDA_URL);
  console.log('Payload: ', JSON.stringify({ experiment_id: experimentId, user_id: userId }));
  console.log('---');

  let response;
  try {
    response = await fetch(LIFECYCLE_LAMBDA_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ experiment_id: experimentId, user_id: userId }),
    });
  } catch (e) {
    console.error('FETCH FAILED (network/CORS/DNS error — this is what triggers E945):', e.message);
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
    console.error('→ response.ok is false — this would trigger E943 in the browser');
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
    console.error('→ outerJson.statusCode is not 200 — this would trigger E943 in the browser');
    return;
  }

  let innerJson;
  try {
    innerJson = JSON.parse(outerJson.body);
  } catch (e) {
    console.error('→ Failed to parse inner JSON (outerJson.body):', e.message);
    return;
  }

  console.log('Inner JSON:', JSON.stringify(innerJson, null, 2));
  console.log('lifecycle_state:', innerJson.lifecycle_state);
}

testFetch();
