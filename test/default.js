const test = require('tape');

const setup = require('./setup.js');
const MongoDbQueue = require('../').default;

setup().then(({client, db}) => {
  test('first test', function(t) {
    const queue = new MongoDbQueue(db, 'default');
    t.ok(queue, 'Queue created ok');
    t.end();
  });

  test('single round trip', async function(t) {
    const queue = new MongoDbQueue(db, 'default');
    let id;

    id = await queue.add('Hello, World!');
    t.ok(id, 'Received an id for this message');

    const msg = await queue.get();
    t.ok(msg.id, 'Got a msg.id');
    t.equal(typeof msg.id, 'string', 'msg.id is a string');
    t.ok(msg.ack, 'Got a msg.ack');
    t.equal(typeof msg.ack, 'string', 'msg.ack is a string');
    t.ok(msg.tries, 'Got a msg.tries');
    t.equal(typeof msg.tries, 'number', 'msg.tries is a number');
    t.equal(msg.tries, 1, 'msg.tries is currently one');
    t.equal(msg.payload, 'Hello, World!', 'Payload is correct');

    id = await queue.ack(msg.ack);
    t.ok(id, 'Received an id when acking this message');
    t.end();
  });

  test("single round trip, can't be acked again", async function(t) {
    const queue = new MongoDbQueue(db, 'default');
    let id;

    id = await queue.add('Hello, World!');
    t.ok(id, 'Received an id for this message');
    const msg = await queue.get();
    t.ok(msg.id, 'Got a msg.id');
    t.equal(typeof msg.id, 'string', 'msg.id is a string');
    t.ok(msg.ack, 'Got a msg.ack');
    t.equal(typeof msg.ack, 'string', 'msg.ack is a string');
    t.ok(msg.tries, 'Got a msg.tries');
    t.equal(typeof msg.tries, 'number', 'msg.tries is a number');
    t.equal(msg.tries, 1, 'msg.tries is currently one');
    t.equal(msg.payload, 'Hello, World!', 'Payload is correct');
    id = await queue.ack(msg.ack);
    t.ok(id, 'Received an id when acking this message');
    id = await queue.ack(msg.ack)
      .catch((err) => t.ok(err, 'There is an error when acking the message again'));

    t.ok(!id, 'No id received when trying to ack an already deleted message');
    t.end();
  });

  test('client.close()', function(t) {
    t.pass('client.close()');
    client.close();
    t.end();
  });
});
