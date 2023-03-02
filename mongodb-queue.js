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

const crypto = require('crypto')

function id() {
  return crypto.randomBytes(16).toString('hex')
}

function now() {
  return (new Date()).toISOString()
}

function nowPlusSecs(secs) {
  return (new Date(Date.now() + secs * 1000)).toISOString()
}

module.exports = class Queue {
  constructor(db, name, opts) {
    if (!db) {
      throw new Error("mongodb-queue: provide a mongodb.MongoClient.db")
    }
    if (!name) {
      throw new Error("mongodb-queue: provide a queue name")
    }
    opts = opts || {}

    this.db = db
    this.name = name
    this.col = db.collection(name)
    this.visibility = opts.visibility || 30
    this.delay = opts.delay || 0

    if (opts.deadQueue) {
      this.deadQueue = opts.deadQueue
      this.maxRetries = opts.maxRetries || 5
    }
  }

  async createIndexes() {
    await Promise.all([
      this.col.createIndex({deleted: 1, visible: 1}),
      this.col.createIndex({ack: 1}, {unique: true, sparse: true}),
      this.col.createIndex({deleted: 1}, {sparse: true})
    ])
  }

  async add(payload, opts = {}) {
    const delay = opts.delay || this.delay
    const visible = delay ? nowPlusSecs(delay) : now()

    const msgs = []
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

  async get(opts = {}) {
    const visibility = opts.visibility || this.visibility
    const query = {
      deleted: null,
      visible: {$lte: now()},
    }
    const sort = {
      _id: 1
    }
    const update = {
      $inc: {tries: 1},
      $set: {
        ack: id(),
        visible: nowPlusSecs(visibility),
      }
    }
    const options = {
      sort: sort,
      returnDocument: 'after'
    }

    const result = await this.col.findOneAndUpdate(query, update, options)
    let msg = result.value
    if (!msg) return

    // convert to an external representation
    msg = {
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
      await this.deadQueue.add(msg)
      await this.ack(msg.ack)
      return this.get()
    }

    return msg
  }

  async ping(ack, opts = {}) {
    const visibility = opts.visibility || this.visibility
    const query = {
      ack: ack,
      visible: {$gt: now()},
      deleted: null,
    }
    const update = {
      $set: {
        visible: nowPlusSecs(visibility)
      }
    }
    const options = {
      returnDocument: 'after'
    }

    if (opts.resetTries) {
      update.$set.tries = 0
    }

    const msg = await this.col.findOneAndUpdate(query, update, options)
    if (!msg.value) {
      throw new Error("Queue.ping(): Unidentified ack  : " + ack)
    }
    return '' + msg.value._id
  }

  async ack(ack) {
    const query = {
      ack: ack,
      visible: {$gt: now()},
      deleted: null,
    }
    const update = {
      $set: {
        deleted: now(),
      }
    }
    const options = {
      returnDocument: 'after'
    }
    const msg = await this.col.findOneAndUpdate(query, update, options)
    if (!msg.value) {
      throw new Error("Queue.ack(): Unidentified ack : " + ack)
    }
    return '' + msg.value._id
  }

  async clean() {
    const query = {
      deleted: {$exists: true},
    }

    return this.col.deleteMany(query)
  }

  async total() {
    return this.col.countDocuments()
  }

  async size() {
    const query = {
      deleted: null,
      visible: {$lte: now()},
    }

    return this.col.countDocuments(query)
  }

  async inFlight() {
    const query = {
      ack: {$exists: true},
      visible: {$gt: now()},
      deleted: null,
    }

    return this.col.countDocuments(query)
  }

  async done() {
    const query = {
      deleted: {$exists: true},
    }

    return this.col.countDocuments(query)
  }
}
