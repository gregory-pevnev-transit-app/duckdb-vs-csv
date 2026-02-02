import * as fs from 'fs-extra';
import * as path from 'path';

import { DuckDBInstance } from "@duckdb/node-api";

const FEED_CODE = 'VICMBAU';

const WORKSPACE_DIR = path.resolve('workspace');
const INPUT_FILE = path.join(WORKSPACE_DIR, `${FEED_CODE}.duckdb`);
const OUTPUT_FILE = path.join(WORKSPACE_DIR, `${FEED_CODE}-export.duckdb`);

const TABLE_NAME = 'trips';

const TABLE_COLUMNS = `
    trip_id VARCHAR,
    raw_trip_id VARCHAR,
    raw_trip_headsign VARCHAR,
    raw_trip_short_name VARCHAR,
    rt_route_id VARCHAR,
    rt_trip_id VARCHAR,
    direction_id VARCHAR,
    rt_trip_headsign VARCHAR,
    trip_direction_id VARCHAR,
    rt_trip_direction_id VARCHAR,
    block_id VARCHAR,
    rt_block_id VARCHAR,
    trip_itinerary_id VARCHAR,
    rt_route_data VARCHAR,
    trip_short_name VARCHAR,
    trip_exclude_in_route_view VARCHAR,
    wheelchair_accessible VARCHAR,
    trip_multimodal_routing_enabled VARCHAR,
    route_id VARCHAR,
    service_id VARCHAR,
    shape_id VARCHAR,
    trip_headsign VARCHAR,
    trip_direction_headsign VARCHAR,
    trip_merged_headsign VARCHAR,
    trip_branch_code VARCHAR,
    rt_trip_branch_code VARCHAR,
    direction_arrow VARCHAR,
    itinerary_index VARCHAR,
    bikes_allowed VARCHAR,
    arrival_times VARCHAR,
    departure_times VARCHAR,
    start_pickup_drop_off_windows VARCHAR,
    end_pickup_drop_off_windows VARCHAR,
    extra_value_by_key VARCHAR
`;
const COLUMN_NAMES = TABLE_COLUMNS.split(',').map(colDef => colDef.trim().split(' ')[0]).filter(colName => colName.length > 0);

async function main() {
  const allRows = [];

  //
  // 1. Import
  //

  console.log('Importing', process.memoryUsage());

  const importDB = await DuckDBInstance.create(INPUT_FILE, {
    threads: '1',
    max_temp_directory_size: '0MB',
  });
  const importConn = await importDB.connect();

  const importer = await importConn.stream(`SELECT * FROM ${TABLE_NAME};`);

  let chunk = await importer.fetchChunk();
  while (chunk !== null && chunk.rowCount > 0) {
    const rows = chunk.getRowObjects(COLUMN_NAMES);
    allRows.push(...rows);

    chunk = await importer.fetchChunk();
  }

  importConn.closeSync();
  importDB.closeSync();

  //
  // 2. Process (Just iterate and count)
  //

  console.log('Processing', process.memoryUsage());
  let count = 0;
  for (const trip of allRows) {
    if (trip.trip_id) {
      count++;
    }
  }
  console.log(count);

  //
  // 3. Export
  //

  console.log('Exporting', process.memoryUsage());

  try {
    await fs.remove(OUTPUT_FILE);
  } catch (error) {}

  const exportDB = await DuckDBInstance.create(':memory:', {
    threads: '1',
    max_temp_directory_size: '0MB',
  });
  const exportConn = await exportDB.connect();

  await exportConn.run(`ATTACH ':memory:' AS db (STORAGE_VERSION 'v1.4.4', ROW_GROUP_SIZE 1228800000, BLOCK_SIZE 262144, COMPRESS TRUE);`);
  await exportConn.run('USE db;');

  await exportConn.run(`CREATE TABLE ${TABLE_NAME} (${TABLE_COLUMNS});`);

  const appender = await exportConn.createAppender(TABLE_NAME);

  let writeCount = 0;
  for (const row of allRows) {
    // Just a good AVG value to prevent double-bufferring (Node.js and DuckDB) -> Does NOT cause disk IO
    if (writeCount > 0 && ((writeCount % (100000)) === 0)) {
      appender.flushSync();
    }

    for (const column of Object.values(row)) {
      appender.appendVarchar(column?.toString() ?? '');
    }
    appender.endRow();

    writeCount++
  }

  appender.closeSync();

  await exportConn.run(`ATTACH '${OUTPUT_FILE}' AS out_db;`);
  await exportConn.run(`COPY FROM DATABASE db TO out_db;`);

  exportConn.closeSync();
  exportDB.closeSync();

  //
  // 4. Done
  //
  console.log('Done', process.memoryUsage());
}
void main();
