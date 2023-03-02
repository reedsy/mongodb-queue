const test = require('tape')

const setup = require('./setup.js')
const MongoDbQueue = require('../').default

setup().then(({client, db}) => {

    test('clean: check deleted messages are deleted', async function (t) {
        const q = new MongoDbQueue(db, 'clean', {visibility: 3})
        let msg
        let id

        t.equal(await q.size(), 0, 'There is currently nothing on the queue')
        t.equal(await q.total(), 0, 'There is currently nothing in the queue at all')
        await q.clean()
        t.equal(await q.size(), 0, 'There is currently nothing on the queue')
        t.equal(await q.total(), 0, 'There is currently nothing in the queue at all')
        await q.add('Hello, World!')
        await q.clean()
        t.equal(await q.size(), 1, 'Queue size is correct')
        t.equal(await q.total(), 1, 'Queue total is correct')
        msg = await q.get()
        t.ok(msg.id, 'Got a msg.id (sanity check)')
        t.equal(await q.size(), 0, 'Queue size is correct')
        t.equal(await q.total(), 1, 'Queue total is correct')
        await q.clean()
        t.equal(await q.size(), 0, 'Queue size is correct')
        t.equal(await q.total(), 1, 'Queue total is correct')
        id = await q.ack(msg.ack)
        t.ok(id, 'Received an id when acking this message')
        t.equal(await q.size(), 0, 'Queue size is correct')
        t.equal(await q.total(), 1, 'Queue total is correct')
        await q.clean()
        t.equal(await q.size(), 0, 'Queue size is correct')
        t.equal(await q.total(), 0, 'Queue total is correct')

        t.pass('Finished test ok')
        t.end()
    })

    test('client.close()', function(t) {
        t.pass('client.close()')
        client.close()
        t.end()
    })

})
