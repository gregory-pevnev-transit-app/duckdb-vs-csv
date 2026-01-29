import z from 'zod';
import { optionalString, requiredString } from '../helpers/zod-helpers.ts';
import { toJsonObjectFromString } from '../helpers/serializers.ts';

export const BIKES_ALLOWED = {
  NO_INFORMATION: '0',
  ALLOWED: '1',
  NOT_ALLOWED: '2',
} as const;

export const EXCLUDE_IN_ROUTE_VIEW = {
  INCLUDE: '0',
  EXCLUDE: '1',
} as const;

export const TRIP_DIRECTION_ID = {
  OUTBOUND: '0',
  INBOUND: '1',
} as const;

export const WHEELCHAIR_ACCESSIBLE = {
  NO_INFORMATION: '0',
  POSSIBLE: '1',
  NOT_POSSIBLE: '2',
} as const;

export const DIRECTION_ARROW = {
  REGULAR: '-',
  CLOCKWISE: '↻',
  COUNTERCLOCKWISE: '↺',
} as const;

const timeArray = z.array(z.number().int().gte(-1)).min(1);

export const tripFields = {
  trip_id: requiredString,
  raw_trip_id: requiredString,
  raw_trip_headsign: optionalString,
  raw_trip_short_name: optionalString,
  rt_route_id: optionalString,
  rt_trip_id: optionalString,
  direction_id: optionalString,
  rt_trip_headsign: optionalString,
  trip_direction_id: optionalString,
  rt_trip_direction_id: optionalString,
  block_id: optionalString,
  rt_block_id: optionalString,
  trip_itinerary_id: optionalString,
  rt_route_data: optionalString,
  trip_short_name: optionalString,
  trip_exclude_in_route_view: optionalString,
  wheelchair_accessible: z.nativeEnum(WHEELCHAIR_ACCESSIBLE).default(WHEELCHAIR_ACCESSIBLE.NO_INFORMATION),
  trip_multimodal_routing_enabled: optionalString,
  route_id: requiredString,
  service_id: requiredString,
  shape_id: optionalString,
  trip_headsign: optionalString,
  trip_direction_headsign: optionalString,
  trip_merged_headsign: optionalString,
  trip_branch_code: optionalString,
  rt_trip_branch_code: optionalString,
  direction_arrow: z.nativeEnum(DIRECTION_ARROW).default(DIRECTION_ARROW.REGULAR),
  itinerary_index: requiredString,
  bikes_allowed: z.nativeEnum(BIKES_ALLOWED).optional(),
  arrival_times: z.preprocess((s) => (typeof s === 'string' ? JSON.parse(s) : s), timeArray),
  departure_times: z.preprocess((s) => (typeof s === 'string' ? JSON.parse(s) : s), timeArray),
  start_pickup_drop_off_windows: z.preprocess((s) => (typeof s === 'string' ? JSON.parse(s) : s), timeArray),
  end_pickup_drop_off_windows: z.preprocess((s) => (typeof s === 'string' ? JSON.parse(s) : s), timeArray),
  extra_value_by_key: optionalString.refine(toJsonObjectFromString),
};
