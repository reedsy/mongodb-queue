const mongodb = require('mongodb');

const url = 'mongodb://localhost:27017/';
const dbName = 'mongodb-queue';

const collections = [
  'default',
  'delay',
  'multi',
  'visibility',
  'clean',
  'ping',
  'stats1',
  'stats2',
  'queue',
  'dead-queue',
  'queue-2',
  'dead-queue-2',
];

module.exports = async function() {
  const client = new mongodb.MongoClient(url, {useNewUrlParser: true});

  await client.connect();
  const db = client.db(dbName);

  await Promise.all(
    collections.map((col) => db.collection(col).deleteMany()),
  );
  return {client, db};
};
