import * as fs from 'fs-extra';
import * as path from 'path';

import { ItineraryTgtfs } from './itinerary-tgtfs.ts';

const FEED_NAME = 'RTL';

const WORKDIR_PATH = path.resolve('workspace');

async function main() {
  const tgtfs = new ItineraryTgtfs();

  const inputPath = path.join(WORKDIR_PATH, FEED_NAME);
  const outputPath = path.join(WORKDIR_PATH, `${FEED_NAME}-export`);

  await fs.remove(outputPath);
  await fs.mkdirp(outputPath);

  await tgtfs.process(inputPath, outputPath);
}
void main();
