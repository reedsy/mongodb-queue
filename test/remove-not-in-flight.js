const test = require('tape');

const setup = require('./setup.js');
const {MongoDBQueue} = require('../');

setup().then(({client, db}) => {
  const queue = new MongoDBQueue(db, 'remove-not-in-flight', {visibility: 30});

  test('removeNotInFlight: removes a waiting message with a matching payload', async function(t) {
    await queue.add({userId: 'a'}, {delay: 60});

    t.equal(await queue.removeNotInFlight({userId: 'a'}), 1, 'One message removed');
    t.equal(await queue.total(), 0, 'Queue is empty');

    t.end();
  });

  test('removeNotInFlight: removes a processed message', async function(t) {
    await queue.add({userId: 'a'});
    const msg = await queue.get();
    await queue.ack(msg.ack);

    t.equal(await queue.removeNotInFlight({userId: 'a'}), 1, 'One message removed');
    t.equal(await queue.total(), 0, 'Queue is empty');

    t.end();
  });

  test('removeNotInFlight: keeps a message in flight, which can still be acked', async function(t) {
    await queue.add({userId: 'a'});
    const msg = await queue.get();

    t.equal(await queue.removeNotInFlight({userId: 'a'}), 0, 'Nothing removed');
    t.equal(await queue.ack(msg.ack), msg.id, 'Message acked');
    await queue.clean();

    t.end();
  });

  test('removeNotInFlight: removes a message whose visibility has expired', async function(t) {
    await queue.add({userId: 'a'});
    await queue.get({visibility: 1});

    setTimeout(async function() {
      t.equal(await queue.removeNotInFlight({userId: 'a'}), 1, 'One message removed');
      t.equal(await queue.total(), 0, 'Queue is empty');
      t.end();
    }, 1500);
  });

  test('removeNotInFlight: only removes messages with a matching payload', async function(t) {
    await queue.add({userId: 'a', kind: 'x'});
    await queue.add({userId: 'b', kind: 'x'});

    t.equal(await queue.removeNotInFlight({userId: 'a', kind: 'x'}), 1, 'One message removed');
    const msg = await queue.get();
    t.deepEqual(msg.payload, {userId: 'b', kind: 'x'}, 'The other message is left');
    await queue.ack(msg.ack);
    await queue.clean();

    t.end();
  });

  test('removeNotInFlight: matches an operator-like value literally', async function(t) {
    await queue.add({userId: 'a'});

    t.equal(await queue.removeNotInFlight({userId: {$exists: true}}), 0, 'Nothing removed');
    t.equal(await queue.total(), 1, 'Message is left');
    await queue.removeNotInFlight({userId: 'a'});

    t.end();
  });

  test('removeNotInFlight: refuses an empty or undefined match', async function(t) {
    await queue.add({userId: 'a'});

    const emptyError = await queue.removeNotInFlight({}).catch((err) => err);
    t.ok(emptyError instanceof Error, 'Got an error for an empty payload');
    const undefinedError = await queue.removeNotInFlight({userId: undefined}).catch((err) => err);
    t.ok(undefinedError instanceof Error, 'Got an error for an undefined value');
    t.equal(await queue.total(), 1, 'Nothing removed');
    await queue.removeNotInFlight({userId: 'a'});

    t.end();
  });

  test('client.close()', function(t) {
    t.pass('client.close()');
    client.close();
    t.end();
  });
});
