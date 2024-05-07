const test = require('tape');
const {timeout} = require('./_timeout');

const setup = require('./setup.js');
const {MongoDBQueue} = require('../');

setup().then(({client, db}) => {
  test('first test', function(t) {
    const queue = new MongoDBQueue(db, 'stats');
    t.ok(queue, 'Queue created ok');
    t.end();
  });

  test('stats for a single message added, received and acked', async function(t) {
    const q = new MongoDBQueue(db, 'stats1');

    const id = await q.add('Hello, World!');
    t.ok(id, 'Received an id for this message');
    t.equal(await q.total(), 1, 'Total number of messages is one');
    t.equal(await q.size(), 1, 'Size of queue is one');
    t.equal(await q.inFlight(), 0, 'There are no inFlight messages');
    t.equal(await q.done(), 0, 'There are no done messages');
    const msg = await q.get();
    t.equal(await q.total(), 1, 'Total number of messages is still one');
    t.equal(await q.size(), 0, 'Size of queue is now zero (ie. none to come)');
    t.equal(await q.inFlight(), 1, 'There is one inflight message');
    t.equal(await q.done(), 0, 'There are still no done messages');
    // now ack that message
    await q.ack(msg.ack);
    t.equal(await q.total(), 1, 'Total number of messages is again one');
    t.equal(await q.size(), 0, 'Size of queue is still zero (ie. none to come)');
    t.equal(await q.inFlight(), 0, 'There are no inflight messages anymore');
    t.equal(await q.done(), 1, 'There is now one processed message');

    t.end();
  });

  // ToDo: add more tests for adding a message, getting it and letting it lapse
  // then re-checking all stats.

  test('stats for a single message added, received, timed-out and back on queue', async function(t) {
    const q = new MongoDBQueue(db, 'stats2', {visibility: 3});

    const id = await q.add('Hello, World!');
    t.ok(id, 'Received an id for this message');
    t.equal(await q.total(), 1, 'Total number of messages is one');
    t.equal(await q.size(), 1, 'Size of queue is one');
    t.equal(await q.inFlight(), 0, 'There are no inFlight messages');
    t.equal(await q.done(), 0, 'There are no done messages');
    // let's set one to be inFlight
    await q.get();
    // msg is ignored, we don't care about the message here
    await timeout(4000);
    t.equal(await q.total(), 1, 'Total number of messages is still one');
    t.equal(await q.size(), 1, 'Size of queue is still at one');
    t.equal(await q.inFlight(), 0, 'There are no inflight messages again');
    t.equal(await q.done(), 0, 'There are still no done messages');

    t.end();
  });

  test('client.close()', function(t) {
    t.pass('client.close()');
    client.close();
    t.end();
  });
});
