const test = require('tape');

const setup = require('./setup.js');
const {MongoDBQueue} = require('../');

setup().then(({client, db}) => {
  const queue = new MongoDBQueue(db, 'remove-not-in-flight', {visibility: 30});

  test('add: stores the given id', async function(t) {
    t.equal(await queue.add('Hello', {id: 'a'}), 'a', 'Returns the given id');
    const msg = await queue.get();
    t.equal(msg.id, 'a', 'Message has the given id');
    await queue.ack(msg.ack);
    await queue.clean();

    t.end();
  });

  test('add: rejects a second message with the same id', async function(t) {
    await queue.add('Hello', {id: 'a'});

    const error = await queue.add('Hello', {id: 'a'}).catch((err) => err);
    t.equal(error && error.code, 11000, 'Got a duplicate key error');
    await queue.removeNotInFlight('a');

    t.end();
  });

  test('add: rejects an id for an array payload', async function(t) {
    const error = await queue.add(['Hello', 'World'], {id: 'a'}).catch((err) => err);
    t.ok(error instanceof Error, 'Got an error');
    t.equal(await queue.total(), 0, 'Nothing added');

    t.end();
  });

  test('removeNotInFlight: removes a waiting message', async function(t) {
    await queue.add('Hello', {id: 'a', delay: 60});

    t.equal(await queue.removeNotInFlight('a'), true, 'Message removed');
    t.equal(await queue.total(), 0, 'Queue is empty');

    t.end();
  });

  test('removeNotInFlight: removes a processed message', async function(t) {
    await queue.add('Hello', {id: 'a'});
    const msg = await queue.get();
    await queue.ack(msg.ack);

    t.equal(await queue.removeNotInFlight('a'), true, 'Message removed');
    t.equal(await queue.total(), 0, 'Queue is empty');

    t.end();
  });

  test('removeNotInFlight: keeps a message in flight, which can still be acked', async function(t) {
    await queue.add('Hello', {id: 'a'});
    const msg = await queue.get();

    t.equal(await queue.removeNotInFlight('a'), false, 'Nothing removed');
    t.equal(await queue.ack(msg.ack), 'a', 'Message acked');
    await queue.clean();

    t.end();
  });

  test('removeNotInFlight: removes a message whose visibility has expired', async function(t) {
    await queue.add('Hello', {id: 'a'});
    await queue.get({visibility: 1});

    setTimeout(async function() {
      t.equal(await queue.removeNotInFlight('a'), true, 'Message removed');
      t.equal(await queue.total(), 0, 'Queue is empty');
      t.end();
    }, 1500);
  });

  test('removeNotInFlight: only removes the message with that id', async function(t) {
    await queue.add('Hello', {id: 'a'});
    await queue.add('World', {id: 'b'});

    t.equal(await queue.removeNotInFlight('a'), true, 'Message removed');
    const msg = await queue.get();
    t.equal(msg.id, 'b', 'The other message is left');
    await queue.ack(msg.ack);
    await queue.clean();

    t.end();
  });

  test('removeNotInFlight: refuses a missing id', async function(t) {
    for (const id of [undefined, null, '']) {
      const error = await queue.removeNotInFlight(id).catch((err) => err);
      t.ok(error instanceof Error, `Got an error for ${JSON.stringify(id)}`);
    }

    t.end();
  });

  test('client.close()', function(t) {
    t.pass('client.close()');
    client.close();
    t.end();
  });
});
