/**
 *
 * mongodb-queue.js - Use your existing MongoDB as a local queue.
 *
 * Copyright (c) 2014 Andrew Chilton
 * - http://chilts.org/
 * - andychilton@gmail.com
 *
 * License: http://chilts.mit-license.org/2014/
 *
 **/

import {Collection, CreateIndexesOptions, Db, Filter, FindOneAndUpdateOptions, ObjectId, Sort, UpdateFilter, WithId} from 'mongodb';

function now(): string {
  return (new Date()).toISOString();
}

function nowPlusSecs(secs: number): string {
  return (new Date(Date.now() + secs * 1000)).toISOString();
}

export type QueueOptions = {
  visibility?: number;
  delay?: number;
  deadQueue?: MongoDBQueue;
  maxRetries?: number;
  expireAfterSeconds?: number;
};

export type AddOptions = {
  delay?: number;
};

export type GetOptions = {
  visibility?: number;
};

export type PingOptions = {
  visibility?: number;
  resetTries?: boolean;
  resetAck?: boolean;
};

export type BaseMessage<T = any> = {
  payload: T;
  visible: string;
};

export type Message<T = any> = BaseMessage<T> & {
  ack: string;
  tries: number;
  deleted?: Date;
};

export type ExternalMessage<T = any> = {
  id: string;
  ack: string;
  payload: T;
  tries: number;
};

export class MongoDBQueue<T = any> {
  private readonly col: Collection<Partial<Message<T>>>;
  private readonly visibility: number;
  private readonly delay: number;
  private readonly maxRetries: number;
  private readonly deadQueue: MongoDBQueue;
  private readonly expireAfterSeconds: number;

  public constructor(db: Db, name: string, opts: QueueOptions = {}) {
    if (!db) {
      throw new Error('mongodb-queue: provide a mongodb.MongoClient.db');
    }
    if (!name) {
      throw new Error('mongodb-queue: provide a queue name');
    }

    this.col = db.collection(name);
    this.visibility = opts.visibility || 30;
    this.delay = opts.delay || 0;
    this.expireAfterSeconds = opts.expireAfterSeconds;

    if (opts.deadQueue) {
      this.deadQueue = opts.deadQueue;
      this.maxRetries = opts.maxRetries || 5;
    }
  }

  public async createIndexes(): Promise<void> {
    const deletedOptions: CreateIndexesOptions = {sparse: true};
    if (typeof this.expireAfterSeconds === 'number') {
      deletedOptions.expireAfterSeconds = this.expireAfterSeconds;
    }

    await Promise.all([
      this.col.createIndex({visible: 1}, {sparse: true}),
      this.col.createIndex({ack: 1}, {unique: true, sparse: true}),
      this.col.createIndex({deleted: 1}, deletedOptions),

      // Index for efficient counts on in-flight
      this.col.createIndex({visible: 1, ack: 1}, {
        partialFilterExpression: {
          visible: {$exists: true},
          ack: {$exists: true},
        },
      }),
    ]);
  }

  public async add(payload: T | T[], opts: AddOptions = {}): Promise<string> {
    const delay = opts.delay || this.delay;
    const visible = delay ? nowPlusSecs(delay) : now();

    const msgs: BaseMessage<T>[] = [];
    if (payload instanceof Array) {
      if (payload.length === 0) {
        throw new Error('Queue.add(): Array payload length must be greater than 0');
      }
      payload.forEach(function(payload) {
        msgs.push({
          visible: visible,
          payload: payload,
        });
      });
    } else {
      msgs.push({
        visible: visible,
        payload: payload,
      });
    }

    const results = await this.col.insertMany(msgs, {ignoreUndefined: true});
    if (payload instanceof Array) return '' + results.insertedIds;
    return '' + results.insertedIds[0];
  }

  public async get(opts: GetOptions = {}): Promise<ExternalMessage<T> | null> {
    const visibility = opts.visibility || this.visibility;
    const query: Filter<Partial<Message<T>>> = {
      visible: {$lte: now()},
    };
    const sort: Sort = {
      visible: 1,
    };
    const update: UpdateFilter<Message<T>> = {
      $inc: {tries: 1},
      $set: {
        ack: new ObjectId().toHexString(),
        visible: nowPlusSecs(visibility),
      },
    };
    const options = {
      sort: sort,
      returnDocument: 'after',
      includeResultMetadata: true,
    } satisfies FindOneAndUpdateOptions;

    const result = await this.col.findOneAndUpdate(query, update, options);
    const msg = result.value as WithId<Message<T>>;
    if (!msg) return null;

    // convert to an external representation
    const externalMessage: ExternalMessage<T> = {
      // convert '_id' to an 'id' string
      id: '' + msg._id,
      ack: msg.ack,
      payload: msg.payload,
      tries: msg.tries,
    };

    // check the tries
    if (this.deadQueue && msg.tries > this.maxRetries) {
      // So:
      // 1) add this message to the deadQueue
      // 2) ack this message from the regular queue
      // 3) call ourself to return a new message (if exists)
      await this.deadQueue.add(externalMessage);
      await this.ack(msg.ack);
      return this.get();
    }

    return externalMessage;
  }

  public async ping(ack: string, opts: PingOptions = {}): Promise<string> {
    const visibility = opts.visibility || this.visibility;
    const query: Filter<Partial<Message<T>>> = {
      ack: ack,
      visible: {$gt: now()},
    };
    const update: UpdateFilter<Message<T>> = {
      $set: {
        visible: nowPlusSecs(visibility),
      },
    };
    const options = {
      returnDocument: 'after',
      includeResultMetadata: true,
    } satisfies FindOneAndUpdateOptions;

    if (opts.resetTries) {
      update.$set = {
        ...update.$set,
        tries: 0,
      };
    }

    if (opts.resetAck) {
      update.$unset = {ack: 1};
    }

    const msg = await this.col.findOneAndUpdate(query, update, options);
    if (!msg.value) {
      throw new Error('Queue.ping(): Unidentified ack  : ' + ack);
    }
    return '' + msg.value._id;
  }

  public async ack(ack: string): Promise<string> {
    const query: Filter<Partial<Message<T>>> = {
      ack: ack,
      visible: {$gt: now()},
    };
    const update: UpdateFilter<Message<T>> = {
      $set: {
        deleted: new Date(),
      },
      $unset: {
        visible: 1,
      },
    };
    const options = {
      returnDocument: 'after',
      includeResultMetadata: true,
    } satisfies FindOneAndUpdateOptions;
    const msg = await this.col.findOneAndUpdate(query, update, options);
    if (!msg.value) {
      throw new Error('Queue.ack(): Unidentified ack : ' + ack);
    }
    return '' + msg.value._id;
  }

  public async clean(): Promise<void> {
    const query = {
      deleted: {$exists: true},
    };

    await this.col.deleteMany(query);
  }

  public async total(): Promise<number> {
    return this.col.countDocuments();
  }

  public async size(): Promise<number> {
    return this.col.countDocuments({
      visible: {$lte: now()},
    });
  }

  public async inFlight(): Promise<number> {
    return this.col.countDocuments({
      // For some unknown reason, MongoDB refuses to use the partial index with
      // {$exists: true}, but *will* use it if we use {$gt: ''}
      // https://www.mongodb.com/community/forums/t/partial-index-is-not-used-during-search/290507/2
      ack: {$gt: ''},
      visible: {$gt: now()},
    });
  }

  public async done(): Promise<number> {
    return this.col.countDocuments({
      deleted: {$exists: true},
    });
  }
}
