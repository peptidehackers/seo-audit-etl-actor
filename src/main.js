import { Actor } from 'apify';
import { processZip } from './etl.js';

await Actor.main(async () => {
  const input = await Actor.getInput();
  const { client, domain, runDate, zipUrl } = input || {};

  // Always log and store input so you can see it in the run
  Actor.log.info('Input received', {
    client,
    domain,
    runDate,
    hasZipUrl: !!zipUrl
  });
  await Actor.setValue('INPUT_ECHO.json', input || {});

  if (!client || !domain || !runDate || !zipUrl) {
    throw new Error('Missing required input: client, domain, runDate, zipUrl. Paste JSON under the Input tab, not Run options.');
  }

  Actor.log.info('Downloading ZIP…');
  const fetchImpl = globalThis.fetch;

  // Run ETL (download → unzip → parse → aggregate → score)
  const result = await processZip({ client, domain, runDate, zipUrl, fetchImpl });

  Actor.log.info('Processing finished. Writing outputs…');
  await Actor.setValue('normalized_audit.json', result.normalized_audit);
  await Actor.setValue('scores.json', result.scores);
  await Actor.setValue('etl_manifest.json', result.manifest);

  await Actor.setValue('OUTPUT', {
    normalized: 'normalized_audit.json',
    scores: 'scores.json',
    etl_manifest: 'etl_manifest.json'
  });

  Actor.log.info('Done. Check Key-Value Store for outputs.');
});
