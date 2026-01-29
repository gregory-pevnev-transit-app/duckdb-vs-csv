import { z } from 'zod';

import { tripFields } from '../tgtfs-types/trip.ts';
import { type ForeignKeyTable, TgtfsTableName } from '../tgtfs-types/common.ts';

import { makeOneIndexTable } from './make-table.ts';

export const tripParser = z.object(tripFields);

export type Trip = z.infer<typeof tripParser>;

export const Trips = makeOneIndexTable({
  tableName: TgtfsTableName.TRIPS,
  fields: tripParser,
  primaryKey: ['trip_id'],
  foreignKeys: new Map<keyof Trip, ForeignKeyTable>([]),
});

export type TripTable = ReturnType<typeof Trips>;

export { tripFields } from '../tgtfs-types/trip.ts';
