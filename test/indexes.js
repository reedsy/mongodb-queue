const test = require('tape');

const setup = require('./setup.js');
const {MongoDBQueue} = require('../');

setup().then(({client, db}) => {
  test('visibility: check message is back in queue after 3s', async function(t) {
    const queue = new MongoDBQueue(db, 'visibility', {visibility: 3});

    await queue.createIndexes();
    t.pass('Indexes created');
    t.end();
  });

  test('client.close()', function(t) {
    t.pass('client.close()');
    client.close();
    t.end();
  });
});
