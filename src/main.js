import Apify from 'apify';
import { processZip } from './etl.js';

Apify.main(async () => {
  const input = await Apify.getInput();
  const { client, domain, runDate, zipUrl } = input || {};
  if (!client || !domain || !runDate || !zipUrl) {
    throw new Error('Missing required input fields: client, domain, runDate, zipUrl');
  }

  const fetchImpl = globalThis.fetch;
  const result = await processZip({ client, domain, runDate, zipUrl, fetchImpl });

  await Apify.setValue('normalized_audit.json', result.normalized_audit);
  await Apify.setValue('scores.json', result.scores);
  await Apify.setValue('etl_manifest.json', result.manifest);

  // small console summary
  await Apify.setValue('OUTPUT', {
    normalized: 'normalized_audit.json',
    scores: 'scores.json',
    etl_manifest: 'etl_manifest.json'
  });
});
