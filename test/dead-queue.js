const test = require('tape');

const setup = require('./setup.js');
const {MongoDBQueue} = require('../');

setup().then(({client, db}) => {
  test('first test', function(t) {
    const queue = new MongoDBQueue(db, 'queue', {visibility: 3, deadQueue: 'dead-queue'});
    t.ok(queue, 'Queue created ok');
    t.end();
  });

  test('single message going over 5 tries, should appear on dead-queue', async function(t) {
    const deadQueue = new MongoDBQueue(db, 'dead-queue');
    const queue = new MongoDBQueue(db, 'queue', {visibility: 1, deadQueue: deadQueue});
    let msg;

    const origId = await queue.add('Hello, World!');
    t.ok(origId, 'Received an id for this message');

    await queue.get();

    for (let i = 1; i <= 5; i++) {
      await queue.get();
      await new Promise((resolve) => setTimeout(function() {
        t.pass(`Expiration #${i}`);
        resolve();
      }, 2 * 1000));
    }

    msg = await queue.get();
    t.ok(!msg, 'No msg received');

    msg = await deadQueue.get();
    t.ok(msg.id, 'Got a message id from the deadQueue');
    t.equal(msg.payload.id, origId, 'Got the same message id as the original message');
    t.equal(msg.payload.payload, 'Hello, World!', 'Got the same as the original message');
    t.equal(msg.payload.tries, 6, 'Got the tries as 6');

    t.end();
  });

  test('client.close()', function(t) {
    t.pass('client.close()');
    client.close();
    t.end();
  });
});
