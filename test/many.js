const test = require('tape');

const setup = require('./setup.js');
const MongoDbQueue = require('../').default;

const total = 250;

setup().then(({client, db}) => {
  test('many: add ' + total + ' messages, get ' + total + ' back', async function(t) {
    const queue = new MongoDbQueue(db, 'many');
    const msgs = [];
    const msgsToQueue = [];

    for (let i = 0; i < total; i++) {
      msgsToQueue.push('no=' + i);
    }
    await queue.add(msgsToQueue);
    t.pass('All ' + total + ' messages sent to MongoDB');

    async function getOne() {
      const msg = await queue.get();
      if (!msg) return t.fail('Failed getting a message');
      msgs.push(msg);
      if (msgs.length !== total) return getOne();
      t.pass('Received all ' + total + ' messages');
    }
    await getOne();

    await Promise.all(
      msgs.map((msg) => queue.ack(msg.ack)),
    );

    t.pass('Acked all ' + total + ' messages');
    t.pass('Finished test ok');
    t.end();
  });

  test('many: add no messages, receive err in callback', async function(t) {
    const queue = new MongoDbQueue(db, 'many');
    await queue.add([])
      .catch(() => t.pass('got error'));
    t.end();
  });

  test('client.close()', function(t) {
    t.pass('client.close()');
    client.close();
    t.end();
  });
});
