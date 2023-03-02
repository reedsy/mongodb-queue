const test = require('tape')
const {timeout} = require('./_timeout')

const setup = require('./setup.js')
const MongoDbQueue = require('../')

setup().then(({client, db}) => {

    test('visibility: check message is back in queue after 3s', async function (t) {
        const queue = new MongoDbQueue(db, 'visibility', {visibility: 3})
        let msg

        await queue.add('Hello, World!')
        msg = await queue.get()
        t.ok(msg.id, 'Got a msg.id (sanity check)')
        await timeout(4_000)
        msg = await queue.get()
        // yes, there should be a message on the queue again
        t.ok(msg.id, 'Got a msg.id (sanity check)')
        await queue.ack(msg.ack)
        msg = await queue.get()
        t.ok(!msg, 'No msg received')

        t.pass('Finished test ok')
        t.end()
    })

    test("visibility: check that a late ack doesn't remove the msg", async function (t) {
        const queue = new MongoDbQueue(db, 'visibility', {visibility: 3})
        let originalAck
        let msg

        await queue.add('Hello, World!')
        msg = await queue.get()
        t.ok(msg.id, 'Got a msg.id (sanity check)')
        // remember this original ack
        originalAck = msg.ack
        // wait over 3s so the msg returns to the queue
        await timeout(4_000)

        t.pass('Back from timeout, now acking the message')

        // now ack the message but too late - it shouldn't be deleted
        msg = await queue.ack(msg.ack)
            .catch((err) => t.ok(err, 'Got an error when acking the message late'))
        t.ok(!msg, 'No message was updated')
        msg = await queue.get()
        // the message should now be able to be retrieved, with a new 'ack' id
        t.ok(msg.id, 'Got a msg.id (sanity check)')
        t.notEqual(msg.ack, originalAck, 'Original ack and new ack are different')

        // now ack this new retrieval
        await queue.ack(msg.ack)
        msg = await queue.get()

        // no more messages
        t.ok(!msg, 'No msg received')

        t.pass('Finished test ok')
        t.end()
    })

    test("visibility: check visibility option overrides the queue visibility", async function (t) {
        const queue = new MongoDbQueue(db, 'visibility', {visibility: 2})
        let msg

        await queue.add('Hello, World!')
        msg = await queue.get({visibility: 4})
        t.ok(msg.id, 'Got a msg.id (sanity check)')
        // wait over 2s so the msg would normally have returns to the queue
        await timeout(3_000)
        msg = await queue.get()
        t.ok(!msg, 'No msg received')
        // wait 2s so the msg should have returns to the queue
        await timeout(2_000)
        msg = await queue.get()
        t.ok(msg.id, 'Got a msg.id (sanity check)')
        await queue.ack(msg.ack)
        msg = await queue.get()
        // no more messages
        t.ok(!msg, 'No msg received')

        t.pass('Finished test ok')
        t.end()
    })

    test('client.close()', function (t) {
        t.pass('client.close()')
        client.close()
        t.end()
    })

})
