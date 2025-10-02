// src/main.js  (Apify SDK v3+ compatible)
import { Actor } from 'apify';
import { processZip } from './etl.js';

await Actor.main(async () => {
  // Read input from Apify (client, domain, runDate, zipUrl)
  const input = await Actor.getInput();
  const { client, domain, runDate, zipUrl } = input || {};

  if (!client || !domain || !runDate || !zipUrl) {
    throw new Error('Missing required input fields: client, domain, runDate, zipUrl');
  }

  // Use global fetch provided by the platform
  const fetchImpl = globalThis.fetch;

  // Do the ETL (download ZIP → unzip → parse → aggregate → score)
  const result = await processZip({ client, domain, runDate, zipUrl, fetchImpl });

  // Save outputs to the run’s Key-Value Store
  await Actor.setValue('normalized_audit.json', result.normalized_audit);
  await Actor.setValue('scores.json', result.scores);
  await Actor.setValue('etl_manifest.json', result.manifest);

  // Small summary for the run console
  await Actor.setValue('OUTPUT', {
    normalized: 'normalized_audit.json',
    scores: 'scores.json',
    etl_manifest: 'etl_manifest.json'
  });
});
