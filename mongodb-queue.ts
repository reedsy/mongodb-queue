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

import {randomBytes} from 'crypto';
import {Collection, Db, Filter, FindOneAndUpdateOptions, Sort, UpdateFilter, WithId} from 'mongodb';

function id(): string {
  return randomBytes(16).toString('hex')
}

function now(): string {
  return (new Date()).toISOString()
}

function nowPlusSecs(secs: number): string {
  return (new Date(Date.now() + secs * 1000)).toISOString()
}

export type QueueOptions = {
  visibility?: number;
  delay?: number;
  deadQueue?: Queue;
  maxRetries?: number;
}

export type AddOptions = {
  delay?: number;
}

export type GetOptions = {
  visibility?: number;
}

export type PingOptions = {
  visibility?: number;
  resetTries?: boolean;
}

export type BaseMessage<T extends any = any> = {
  payload: T;
  visible: string;
}

export type Message<T extends any = any> = BaseMessage<T> & {
  ack: string;
  tries: number;
  deleted?: string;
}

export type ExternalMessage<T extends any = any> = {
  id: string;
  ack: string;
  payload: T;
  tries: number;
}

export default class Queue<T extends any = any> {
  private readonly col: Collection<Partial<Message<T>>>;
  private readonly visibility: number;
  private readonly delay: number;
  private readonly maxRetries: number;
  private readonly deadQueue: Queue;

  constructor(db: Db, name: string, opts: QueueOptions = {}) {
    if (!db) {
      throw new Error("mongodb-queue: provide a mongodb.MongoClient.db")
    }
    if (!name) {
      throw new Error("mongodb-queue: provide a queue name")
    }

    this.col = db.collection(name)
    this.visibility = opts.visibility || 30
    this.delay = opts.delay || 0

    if (opts.deadQueue) {
      this.deadQueue = opts.deadQueue
      this.maxRetries = opts.maxRetries || 5
    }
  }

  async createIndexes(): Promise<void> {
    await Promise.all([
      this.col.createIndex({deleted: 1, visible: 1}),
      this.col.createIndex({ack: 1}, {unique: true, sparse: true}),
      this.col.createIndex({deleted: 1}, {sparse: true})
    ])
  }

  async add(payload: T | T[], opts: AddOptions = {}): Promise<string> {
    const delay = opts.delay || this.delay
    const visible = delay ? nowPlusSecs(delay) : now()

    const msgs: BaseMessage<T>[] = []
    if (payload instanceof Array) {
      if (payload.length === 0) {
        throw new Error('Queue.add(): Array payload length must be greater than 0')
      }
      payload.forEach(function (payload) {
        msgs.push({
          visible: visible,
          payload: payload,
        })
      })
    } else {
      msgs.push({
        visible: visible,
        payload: payload,
      })
    }

    const results = await this.col.insertMany(msgs)
    if (payload instanceof Array) return '' + results.insertedIds
    return '' + results.insertedIds[0]
  }

  async get(opts: GetOptions = {}): Promise<ExternalMessage<T> | null> {
    const visibility = opts.visibility || this.visibility
    const query: Filter<Partial<Message<T>>> = {
      deleted: {$exists: false},
      visible: {$lte: now()},
    }
    const sort: Sort = {
      _id: 1
    }
    const update: UpdateFilter<Message<T>> = {
      $inc: {tries: 1},
      $set: {
        ack: id(),
        visible: nowPlusSecs(visibility),
      }
    }
    const options: FindOneAndUpdateOptions = {
      sort: sort,
      returnDocument: 'after'
    }

    const result = await this.col.findOneAndUpdate(query, update, options)
    const msg = result.value as WithId<Message<T>>;
    if (!msg) return null

    // convert to an external representation
    const externalMessage: ExternalMessage<T> = {
      // convert '_id' to an 'id' string
      id: '' + msg._id,
      ack: msg.ack,
      payload: msg.payload,
      tries: msg.tries,
    }

    // check the tries
    if (this.deadQueue && msg.tries > this.maxRetries) {
      // So:
      // 1) add this message to the deadQueue
      // 2) ack this message from the regular queue
      // 3) call ourself to return a new message (if exists)
      await this.deadQueue.add(externalMessage)
      await this.ack(msg.ack)
      return this.get()
    }

    return externalMessage
  }

  async ping(ack: string, opts: PingOptions = {}): Promise<string> {
    const visibility = opts.visibility || this.visibility
    const query: Filter<Partial<Message<T>>> = {
      ack: ack,
      visible: {$gt: now()},
      deleted: {$exists: false},
    }
    const update: UpdateFilter<Message<T>> = {
      $set: {
        visible: nowPlusSecs(visibility)
      }
    }
    const options: FindOneAndUpdateOptions = {
      returnDocument: 'after'
    }

    if (opts.resetTries) {
      update.$set = {
        ...update.$set,
        tries: 0,
      }
    }

    const msg = await this.col.findOneAndUpdate(query, update, options)
    if (!msg.value) {
      throw new Error("Queue.ping(): Unidentified ack  : " + ack)
    }
    return '' + msg.value._id
  }

  async ack(ack: string): Promise<string> {
    const query: Filter<Partial<Message<T>>> = {
      ack: ack,
      visible: {$gt: now()},
      deleted: {$exists: false},
    }
    const update: UpdateFilter<Message<T>> = {
      $set: {
        deleted: now(),
      }
    }
    const options: FindOneAndUpdateOptions = {
      returnDocument: 'after'
    }
    const msg = await this.col.findOneAndUpdate(query, update, options)
    if (!msg.value) {
      throw new Error("Queue.ack(): Unidentified ack : " + ack)
    }
    return '' + msg.value._id
  }

  async clean(): Promise<void> {
    const query = {
      deleted: {$exists: true},
    }

    await this.col.deleteMany(query)
  }

  async total(): Promise<number> {
    return this.col.countDocuments()
  }

  async size(): Promise<number> {
    return this.col.countDocuments({
      deleted: {$exists: false},
      visible: {$lte: now()},
    })
  }

  async inFlight(): Promise<number> {
    return this.col.countDocuments({
      ack: {$exists: true},
      visible: {$gt: now()},
      deleted: {$exists: false},
    })
  }

  async done(): Promise<number> {
    return this.col.countDocuments({
      deleted: {$exists: true},
    })
  }
}
