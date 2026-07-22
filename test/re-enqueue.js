const test = require('tape');

const setup = require('./setup.js');
const {MongoDBQueue} = require('../');

setup().then(({client, db}) => {
  test('re-enqueue: message becomes immediately available again, with tries and ack reset', async function(t) {
    const queue = new MongoDBQueue(db, 're-enqueue', {visibility: 30});

    const id = await queue.add('Hello, World!');
    const msg = await queue.get();

    const reEnqueuedId = await queue.reEnqueue(msg.ack);
    t.equal(reEnqueuedId, id, 'Re-enqueue keeps the same document id');

    const requeued = await queue.get();
    t.ok(requeued, 'Message is immediately available again');
    t.equal(requeued.id, id, 'Same document id after re-enqueue');
    t.equal(requeued.tries, 1, 'Tries restarted from zero');
    t.notEqual(requeued.ack, msg.ack, 'The old ack no longer applies');
    // ack so it doesn't linger reserved and leak into the next test on this collection
    await queue.ack(requeued.ack);

    t.end();
  });

  test("re-enqueue: can't re-enqueue with a stale or unknown ack", async function(t) {
    const queue = new MongoDBQueue(db, 're-enqueue', {visibility: 30});

    await queue.add('Hello, World!');
    const msg = await queue.get();
    await queue.ack(msg.ack);

    const error = await queue.reEnqueue(msg.ack).catch((err) => err);
    t.ok(error, 'Got an error when re-enqueuing an already-acked message');

    const unknownError = await queue.reEnqueue('unknown-ack').catch((err) => err);
    t.ok(unknownError, 'Got an error when re-enqueuing an unknown ack');

    t.end();
  });

  test('client.close()', function(t) {
    t.pass('client.close()');
    client.close();
    t.end();
  });
});
