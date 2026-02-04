import { z } from "zod/v4";
import { BIKES_ALLOWED, DIRECTION_ARROW, WHEELCHAIR_ACCESSIBLE } from "./csv/tgtfs-types/trip.ts";
import { toJsonObjectFromString } from "./csv/helpers/serializers.ts";

const tripDef = z.object({
  trip_id: z.string().min(1),
  raw_trip_id: z.string().min(1),
  raw_trip_headsign: z.string().min(1).optional(),
  raw_trip_short_name: z.string().min(1).optional(),
  rt_route_id: z.string().min(1).optional(),
  rt_trip_id: z.string().min(1).optional(),
  direction_id: z.string().min(1).optional(),
  rt_trip_headsign: z.string().min(1).optional(),
  trip_direction_id: z.string().min(1).optional(),
  rt_trip_direction_id: z.string().min(1).optional(),
  block_id: z.string().min(1).optional(),
  rt_block_id: z.string().min(1).optional(),
  trip_itinerary_id: z.string().min(1).optional(),
  rt_route_data: z.string().min(1).optional(),
  trip_short_name: z.string().min(1).optional(),
  trip_exclude_in_route_view: z.string().min(1).optional(),
  wheelchair_accessible: z.enum(WHEELCHAIR_ACCESSIBLE).default(WHEELCHAIR_ACCESSIBLE.NO_INFORMATION),
  trip_multimodal_routing_enabled: z.string().min(1).optional(),
  route_id: z.string().min(1),
  service_id: z.string().min(1),
  shape_id: z.string().min(1).optional(),
  trip_headsign: z.string().min(1).optional(),
  trip_direction_headsign: z.string().min(1).optional(),
  trip_merged_headsign: z.string().min(1).optional(),
  trip_branch_code: z.string().min(1).optional(),
  rt_trip_branch_code: z.string().min(1).optional(),
  direction_arrow: z.enum(DIRECTION_ARROW).default(DIRECTION_ARROW.REGULAR),
  itinerary_index: z.string().min(1),
  bikes_allowed: z.enum(BIKES_ALLOWED).optional(),
  arrival_times: z.preprocess((s) => (typeof s === 'string' ? JSON.parse(s) : s), z.array(z.number().int().gte(-1)).min(1)),
  departure_times: z.preprocess((s) => (typeof s === 'string' ? JSON.parse(s) : s), z.array(z.number().int().gte(-1)).min(1)),
  start_pickup_drop_off_windows: z.preprocess((s) => (typeof s === 'string' ? JSON.parse(s) : s), z.array(z.number().int().gte(-1)).min(1)),
  end_pickup_drop_off_windows: z.preprocess((s) => (typeof s === 'string' ? JSON.parse(s) : s), z.array(z.number().int().gte(-1)).min(1)),
  extra_value_by_key: z.string().min(1).optional().refine(toJsonObjectFromString),
});


function zodTypeToDuckDbType(schema: z.core.$ZodType, optional = false, defaultValue?: unknown): string {
  const formatDefault = (defaultValue: unknown) => typeof defaultValue  === 'number' ? `${defaultValue}` : `'${defaultValue}'`;
  const nullableWithDefault = (type: string) => `${type}${optional ? '' : ' NOT NULL'}${defaultValue ? ` DEFAULT ${formatDefault(defaultValue)}` : ''}`
  if (schema instanceof z.ZodString) {
    return nullableWithDefault('VARCHAR');
  }
  if (schema instanceof z.ZodOptional) {
    return zodTypeToDuckDbType(schema._zod.def.innerType, true)
  }
  if (schema instanceof z.ZodDefault) {
    return zodTypeToDuckDbType(schema._zod.def.innerType, optional, schema._zod.def.defaultValue);
  }
  if (schema instanceof z.ZodNumber) {
    const format = schema._zod.bag.format;
    if (typeof format === "string" && format.includes("int")) return nullableWithDefault('INT');
    return nullableWithDefault('FLOAT');
  }
  if (schema instanceof z.ZodEnum) {
    const values = Object.values(schema._zod.def.entries);
    return nullableWithDefault(`ENUM (${values.map(v => `'${v}'`).join(', ')})`)
  }
  if (schema instanceof z.ZodPipe) {
    return zodTypeToDuckDbType(schema._zod.def.out);
  }
  if (schema instanceof z.ZodArray) {
    // Only support one-nested arrays for GTFS purposes.
    const innerType = schema._zod.def.element;
    if (innerType instanceof z.ZodNumber || innerType instanceof z.ZodString) {
      // set optional here since you can't prepend [] with NOT NULL.
      const duckdbType = zodTypeToDuckDbType(innerType, true);
      return nullableWithDefault(`${duckdbType}[]`)
    }
    throw new Error('Only string or number arrays are supported.')
  }
  throw new Error('Unsupported zod type found in tGTFS schema.')
}

function zodTableDefToDuckdbColumns(def: z.ZodObject): string {
  return Object.entries(def.shape).map(([key, schema]) => `${key} ${zodTypeToDuckDbType(schema)}`).join(',\n')
}

function zodTableDefToDuckdbCreateTable(def: z.ZodObject, name: string): string {
  return `CREATE TABLE ${name} (${zodTableDefToDuckdbColumns(def)});`
}

function escapeSingleQuote(str: string) {
  return str.replaceAll('\'', '\'\'')
}

function zodTableDefToReadCsvTypes(def: z.ZodObject): string {
  return Object.entries(def.shape).map(([key, schema]) => `'${key}': '${escapeSingleQuote(zodTypeToDuckDbType(schema))}'`).join(', ')
}

function zodTableDefToReadCsv(def: z.ZodObject, path: string, name: string): string {
  return `SELECT * FROM read_csv('${path}', columns={${zodTableDefToReadCsvTypes(def)}}, header = true);`;
}

console.log(zodTableDefToDuckdbColumns(tripDef))
