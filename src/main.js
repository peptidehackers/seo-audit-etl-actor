// src/main.js — Apify SDK v3
import { Actor, log } from 'apify';
import { processZip } from './etl.js';

await Actor.main(async () => {
  const input = await Actor.getInput();
  const { client, domain, runDate, zipUrl } = input || {};

  // Log and echo input so you can always see what the run received
  log.info('Input received', {
    client,
    domain,
    runDate,
    hasZipUrl: !!zipUrl
  });
  await Actor.setValue('INPUT_ECHO.json', input || {});

  if (!client || !domain || !runDate || !zipUrl) {
    throw new Error(
      'Missing required input: client, domain, runDate, zipUrl. ' +
      'Paste JSON under the Input tab (not only Run options).'
    );
  }

  log.info('Downloading ZIP…');
  const fetchImpl = globalThis.fetch;

  const result = await processZip({ client, domain, runDate, zipUrl, fetchImpl });

  log.info('Processing finished. Writing outputs…');
  await Actor.setValue('normalized_audit.json', result.normalized_audit);
  await Actor.setValue('scores.json', result.scores);
  await Actor.setValue('etl_manifest.json', result.manifest);

  await Actor.setValue('OUTPUT', {
    normalized: 'normalized_audit.json',
    scores: 'scores.json',
    etl_manifest: 'etl_manifest.json'
  });

  log.info('Done. Check Key-Value Store for outputs.');
});
