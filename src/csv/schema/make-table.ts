import { z } from 'zod';
import { importFromCsv } from '../helpers/import.ts';
import { TgtfsParsingError } from './errors.ts';
import { ItineraryTgtfs } from '../itinerary-tgtfs.ts';
import { exportTable, writeCsvRow } from '../helpers/export.ts';
import { TgtfsTableName, type ForeignKeyTable } from '../tgtfs-types/common.ts';

export type OneIndexMap<T> = Map<string, T>;
export type TwoIndexMap<T> = Map<string, OneIndexMap<T>>;

type Entity<S extends z.ZodRawShape> = z.output<z.ZodObject<S>>;
type EntityKey<S extends z.ZodRawShape> = keyof z.infer<z.ZodObject<S>>;

export type OneIndexConfig<GtfsShape extends z.ZodRawShape, S extends GtfsShape> = {
  tableName: TgtfsTableName;
  fields: z.ZodObject<S>;
  /**
   * the primary key(s) of the table. empty array if no primary keys.
   * can be empty - just means that the primary key is a composite of every field
   */
  primaryKey: EntityKey<S>[];
  foreignKeys: Map<EntityKey<S>, ForeignKeyTable>;
  interFeedKeys?: EntityKey<S>[];
  gtfsFields?: z.ZodObject<GtfsShape>;
  additionalValidation?: [errorMessage: string, validator: (obj: Entity<S>) => boolean][];
  transformHeader?: (header: string) => string;
};

export type TwoIndexConfig<GtfsShape extends z.ZodRawShape, S extends GtfsShape> = Omit<
  OneIndexConfig<GtfsShape, S>,
  'primaryKey'
> & {
  /**
   * the primary key is a grouping mechanism. i.e., for fare_leg_rules, many leg rules can have the same
   * leg_group_id, but it is a common operation to get all leg rules with the same leg_group_id.
   * the secondary keys are just the keys that are needed, in conjunction with the primary keys, to determine uniqueness
   */
  primaryKey: [EntityKey<S>, ...EntityKey<S>[]];
  secondaryKey: [EntityKey<S>, ...EntityKey<S>[]];
};

export class OneIndexTable<GtfsShape extends z.ZodRawShape, S extends GtfsShape> {
  readonly #entities: OneIndexMap<Entity<S>>;
  readonly #tgtfs: ItineraryTgtfs;
  readonly #config: OneIndexConfig<GtfsShape, S>;
  readonly #foreignKeys: [EntityKey<S>, ForeignKeyTable][];
  readonly #zodFieldsSchema: z.ZodEffects<z.ZodObject<S>>;
  *[Symbol.iterator](): Generator<Entity<S>> {
    for (const entity of this.#entities.values()) {
      yield entity;
    }
  }

  readonly #getKey: (entity: Entity<S>) => string;
  constructor(schema: OneIndexConfig<GtfsShape, S>, tgtfs: ItineraryTgtfs) {
    // indexed on primary key defined in schema, or by hash of fields.
    this.#entities = new Map();
    this.#tgtfs = tgtfs;
    this.#config = schema;
    this.#foreignKeys = [...this.#config.foreignKeys.entries()].filter(
      ([field]) => !(this.#config.interFeedKeys?.includes(field) && this.#tgtfs.allowInterFeedKeys),
    );
    this.#zodFieldsSchema = schema.fields.superRefine((obj, ctx) => {
      for (const [message, validator] of this.#config.additionalValidation ?? []) {
        if (!validator(obj)) ctx.addIssue({ code: z.ZodIssueCode.custom, message });
      }
    });

    this.#getKey =
      this.#config.primaryKey.length === 0
        ? (entity) =>
            Object.values(entity)
              .map((v) => String(v))
              .join('␟')
        : (entity) => this.#config.primaryKey.map((k) => String(entity[k])).join('␟');
  }

  /**
   * Parse an entity and add it to the table.
   * Sets defaults on the entity, and unless validateLinkedFields is false, checks that all foreign keys are valid.
   */
  addEntity(preEntity: unknown, validateLinkedFields = true): Entity<S> {
    const entity = parseEntity(this.#zodFieldsSchema, this.#config.tableName, preEntity);

    if (validateLinkedFields) {
      for (const [foreignKey, tableName] of this.#foreignKeys) {
        if (!checkForeignKey(this.#tgtfs, tableName, entity[foreignKey])) {
          throw new Error(
            [
              'Trying to create an entity with an invalid foreign key',
              `Foreign field ${String(foreignKey)} does not exist in referenced table ${tableName}`,
              `Invalid value: ${entity[foreignKey]}`,
            ].join('\n'),
          );
        }
      }
    }

    return this.addParsedEntity(entity);
  }

  /**
   * Add a parsed entity to the table.
   * Does not parse or validate foreign keys.
   */
  addParsedEntity(entity: Entity<S>): Entity<S> {
    this.#entities.set(this.#getKey(entity), entity);
    return entity;
  }

  /**
   * Parse multiple entities and add it to the table.
   * Sets defaults on the entities, and unless validateLinkedFields is false, checks that all foreign keys are valid.
   */
  addEntities(preEntities: Iterable<unknown>): Entity<S>[] {
    const toReturn: Entity<S>[] = [];
    for (const entity of preEntities) {
      toReturn.push(this.addEntity(entity));
    }
    return toReturn;
  }

  /**
   * Add multiple entities to the table.
   * Does not parse or validate foreign keys.
   */
  addParsedEntities(entities: Iterable<Entity<S>>): Entity<S>[] {
    const toReturn: Entity<S>[] = [];
    for (const entity of entities) {
      toReturn.push(this.addParsedEntity(entity));
    }
    return toReturn;
  }

  getWithId(id: string): Entity<S> | null {
    return this.#entities.get(id) ?? null;
  }

  deleteEntity(entity: Entity<S>): void {
    this.#entities.delete(this.#getKey(entity));
  }

  deleteEntities(entities: Iterable<Entity<S>>): void {
    for (const entity of entities) {
      this.deleteEntity(entity);
    }
  }

  validateLinkedFields() {
    for (const entity of this) {
      for (const [foreignKey, tableName] of this.#foreignKeys) {
        if (!checkForeignKey(this.#tgtfs, tableName, entity[foreignKey])) {
          throw new Error(
            [
              `Entity with invalid foreign key in table ${this.#config.tableName}`,
              `Foreign field ${String(foreignKey)} does not exist in referenced table ${tableName}`,
              `Invalid value: ${entity[foreignKey]} in entity:`,
              JSON.stringify(entity, null, 2),
            ].join('\n'),
          );
        }
      }
    }
  }

  clear() {
    this.#entities.clear();
  }

  async importFromPath(path: string): Promise<void> {
    await importFromCsv(path, (preEntity: unknown) => {
      this.addEntity(preEntity, false)
    }, this.#config.transformHeader);

    if (!this.#tgtfs.transcodeMode) this.validateLinkedFields();
  }

  async exportToPath(path: string, gtfsOnly = false): Promise<void> {
    const fields: (keyof S)[] =
      gtfsOnly && this.#config.gtfsFields
        ? Object.keys(this.#config.gtfsFields.shape)
        : Object.keys(this.#config.fields.shape);
    await exportTable(path, this[Symbol.iterator](), (r) => writeCsvRow(fields.map((f) => r[f])), fields.join(','));
  }
}

export class TwoIndexTable<GtfsShape extends z.ZodRawShape, S extends GtfsShape> {
  readonly #entities: TwoIndexMap<Entity<S>>;
  readonly #tgtfs: ItineraryTgtfs;
  readonly #config: TwoIndexConfig<GtfsShape, S>;
  readonly #foreignKeys: [EntityKey<S>, ForeignKeyTable][];
  readonly #zodFieldsSchema: z.ZodEffects<z.ZodObject<S>>;
  *[Symbol.iterator](): Generator<Entity<S>> {
    for (const entityBySecondId of this.#entities.values()) {
      for (const entity of entityBySecondId.values()) {
        yield entity;
      }
    }
  }

  *eachByPrimaryKey(): Generator<[Entity<S>, ...Entity<S>[]]> {
    for (const entityBySecondId of this.#entities.values()) {
      const values = [...entityBySecondId.values()];
      if (values.length > 0) yield values as any;
    }
  }

  constructor(schema: TwoIndexConfig<GtfsShape, S>, tgtfs: ItineraryTgtfs) {
    // indexed on primary key defined in schema, or by hash of fields.
    this.#entities = new Map();
    this.#tgtfs = tgtfs;
    this.#config = schema;
    this.#foreignKeys = [...this.#config.foreignKeys.entries()].filter(
      ([field]) => !(this.#config.interFeedKeys?.includes(field) && this.#tgtfs.allowInterFeedKeys),
    );
    this.#zodFieldsSchema = schema.fields.superRefine((obj, ctx) => {
      for (const [message, validator] of this.#config.additionalValidation ?? []) {
        if (!validator(obj)) ctx.addIssue({ code: z.ZodIssueCode.custom, message });
      }
    });
  }

  _getPrimaryKey(entity: Entity<S>) {
    return this.#config.primaryKey.map((field) => String(entity[field])).join('␟');
  }

  _getSecondaryKey(entity: Entity<S>) {
    return this.#config.secondaryKey.map((field) => String(entity[field])).join('␟');
  }

  /**
   * Parse an entity and add it to the table.
   * Sets defaults on the entity, and unless validateLinkedFields is false, checks that all foreign keys are valid.
   */
  addEntity(preEntity: unknown, validateLinkedFields = true): Entity<S> {
    const entity = parseEntity(this.#zodFieldsSchema, this.#config.tableName, preEntity);

    // check foreign keys
    if (validateLinkedFields) {
      for (const [foreignKey, tableName] of this.#foreignKeys) {
        if (!checkForeignKey(this.#tgtfs, tableName, entity[foreignKey])) {
          throw new Error(
            [
              'Trying to create an entity with an invalid foreign key',
              `Foreign field ${String(foreignKey)} does not exist in referenced table ${tableName}`,
              `Invalid value: ${entity[foreignKey]}`,
            ].join('\n'),
          );
        }
      }
    }

    return this.addParsedEntity(entity);
  }

  /**
   * Add a parsed entity to the table.
   * Does not parse or validate foreign keys.
   */
  addParsedEntity(entity: Entity<S>): Entity<S> {
    const entityBySecondaryKey = this.#entities.get(this._getPrimaryKey(entity)) ?? new Map();
    entityBySecondaryKey.set(this._getSecondaryKey(entity), entity);
    this.#entities.set(this._getPrimaryKey(entity), entityBySecondaryKey);
    return entity;
  }

  /**
   * Parse multiple entities and add it to the table.
   * Sets defaults on the entities, and unless validateLinkedFields is false, checks that all foreign keys are valid.
   */
  addEntities(preEntities: Iterable<unknown>): Entity<S>[] {
    const toReturn: Entity<S>[] = [];
    for (const entity of preEntities) {
      toReturn.push(this.addEntity(entity));
    }
    return toReturn;
  }

  /**
   * Add multiple entities to the table.
   * Does not parse or validate foreign keys.
   */
  addParsedEntities(entities: Iterable<Entity<S>>): Entity<S>[] {
    const toReturn: Entity<S>[] = [];
    for (const entity of entities) {
      toReturn.push(this.addParsedEntity(entity));
    }
    return toReturn;
  }

  getWithIds(primaryKey: string, secondaryKey: string): Entity<S> | null {
    return this.#entities.get(primaryKey)?.get(secondaryKey) ?? null;
  }

  getWithFirstId(id: string): Entity<S>[] {
    const entityBySecondId = this.#entities.get(id);
    return entityBySecondId ? [...entityBySecondId.values()] : [];
  }

  deleteEntity(entity: Entity<S>): void {
    this.#entities.get(this._getPrimaryKey(entity))?.delete(this._getSecondaryKey(entity));

    if (this.#entities.get(this._getPrimaryKey(entity))?.size === 0) {
      this.#entities.delete(this._getPrimaryKey(entity));
    }
  }

  deleteEntities(entities: Iterable<Entity<S>>): void {
    for (const entity of entities) {
      this.deleteEntity(entity);
    }
  }

  validateLinkedFields() {
    for (const entity of this) {
      for (const [foreignKey, tableName] of this.#foreignKeys) {
        if (!checkForeignKey(this.#tgtfs, tableName, entity[foreignKey])) {
          throw new Error(
            [
              `Entity with invalid foreign key in table ${this.#config.tableName}`,
              `Foreign field ${String(foreignKey)} does not exist in referenced table ${tableName}`,
              `Invalid value: ${entity[foreignKey]} in entity:`,
              JSON.stringify(entity, null, 2),
            ].join('\n'),
          );
        }
      }
    }
  }

  clear() {
    this.#entities.clear();
  }

  async importFromPath(path: string): Promise<void> {
    await importFromCsv(path, (preEntity: unknown) => this.addEntity(preEntity, false), this.#config.transformHeader);
    if (!this.#tgtfs.transcodeMode) this.validateLinkedFields();
  }

  async exportToPath(path: string, gtfsOnly = false): Promise<void> {
    const fields: (keyof S)[] =
      gtfsOnly && this.#config.gtfsFields
        ? Object.keys(this.#config.gtfsFields.shape)
        : Object.keys(this.#config.fields.shape);
    await exportTable(path, this[Symbol.iterator](), (r) => writeCsvRow(fields.map((f) => r[f])), fields.join(','));
  }
}

export function makeOneIndexTable<GtfsShape extends z.ZodRawShape, S extends GtfsShape>(
  config: OneIndexConfig<GtfsShape, S>,
): (tgtfs: ItineraryTgtfs) => OneIndexTable<GtfsShape, S> {
  return (tgtfs) => new OneIndexTable(config, tgtfs);
}

export function makeTwoIndexTable<GtfsShape extends z.ZodRawShape, S extends GtfsShape>(
  config: TwoIndexConfig<GtfsShape, S>,
): (tgtfs: ItineraryTgtfs) => TwoIndexTable<GtfsShape, S> {
  return (tgtfs) => new TwoIndexTable(config, tgtfs);
}

function checkForeignKey(tgtfs: ItineraryTgtfs, tableName: ForeignKeyTable, id: unknown) {
  // means the schema is optional
  if (id === undefined) {
    return true;
  }

  if (typeof id !== 'string') {
    throw new Error(`Foreign key ${JSON.stringify(id)} is not a string`);
  }

  const referencedTable = tgtfs[tableName];
  return referencedTable.getWithId(id);
}

function parseEntity<T extends z.SomeZodObject>(schema: z.ZodEffects<T>, tableName: string, entity: unknown) {
  const parsed = schema.safeParse(entity, { errorMap });

  if (parsed.success) {
    return parsed.data;
  }
  throw new TgtfsParsingError(tableName, parsed.error, entity);
}

const errorMap: z.ZodErrorMap = (issue, ctx) => {
  switch (issue.code) {
    case 'too_big':
      return { message: `Field ${issue.path} is a number that is too large, the maximum is ${issue.maximum}` };
    case 'too_small':
      return { message: `Field ${issue.path} is a number that is too small, the minimum is ${issue.minimum}` };
    case 'invalid_enum_value':
      return { message: `Field ${issue.path} is not one of the allowed enum values: ${issue.options.join(',')}` };
    case 'invalid_string':
      // eslint-disable-next-line @typescript-eslint/no-base-to-string
      return { message: `Field ${issue.path} is invalid based on ${issue.validation}` };
    case 'invalid_type':
      return {
        message: `Field ${issue.path} is not one of the expected type. The expected type is ${issue.expected}, but we received a value of type ${issue.received}`,
      };
    default:
      return { message: ctx.defaultError };
  }
};
