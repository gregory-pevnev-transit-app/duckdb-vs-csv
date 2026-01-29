import path from 'path';

import { type TripTable, Trips } from './schema/trips.ts';
import { TGTFS_FILE_NAMES } from './tgtfs-types/common.ts';

/**
 * this is just the Tgtfs class in tgtfs.ts, with the following modifications
 *
 * it supports itineraries.jsonl and trips.txt for itineraries
 * it does not handle stop_times, since they are redundant with itineraries
 * it does not provide route<>trip linkages, since the old trip class is no longer used
 * it does not provide stop<>stop_time linkages, since the stop_time class no longer is used
 */
export class ItineraryTgtfs {
  isItineraryTgtfs = true;
  transcodeMode = false;
  allowInterFeedKeys = false;

  trips: TripTable;

  constructor() {
    this.trips = Trips(this);
  }

  async process(inputPath: string, exportPath: string) {
    // 1. Import
    console.log('Importing', process.memoryUsage());
    await this.trips.importFromPath(path.join(inputPath, TGTFS_FILE_NAMES.trips));

    // 2. Process (Just iterate and count)
    console.log('Processing', process.memoryUsage());
    let count = 0;
    for (const trip of this.trips) {
      if (trip.trip_id) {
        count++;
      }
    }
    console.log(count);

    // 3. Export
    console.log('Exporting', process.memoryUsage());
    await this.trips.exportToPath(path.join(exportPath, TGTFS_FILE_NAMES.trips));

    // 4. Done
    console.log('Done', process.memoryUsage());
  }
}
