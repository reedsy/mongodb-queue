const test = require('tape')
const {timeout} = require('./_timeout')

const setup = require('./setup.js')
const MongoDbQueue = require('../').default

setup().then(({client, db}) => {

    test('ping: check a retrieved message with a ping can still be acked', async function (t) {
        const queue = new MongoDbQueue(db, 'ping', {visibility: 5})
        let msg
        let id

        id = await queue.add('Hello, World!')
        t.ok(id, 'There is an id returned when adding a message.')
        // get something now and it shouldn't be there
        msg = await queue.get()
        t.ok(msg.id, 'Got this message id')
        await timeout(4_000)
        // ping this message so it will be kept alive longer, another 5s
        id = await queue.ping(msg.ack)
        t.ok(id, 'Received an id when acking this message')
        await timeout(4_000)
        id = await queue.ack(msg.ack)
        t.ok(id, 'Received an id when acking this message')
        msg = await queue.get()
        t.ok(!msg, 'No message when getting from an empty queue')

        t.pass('Finished test ok')
        t.end()
    })

    test("ping: check that an acked message can't be pinged", async function (t) {
        const queue = new MongoDbQueue(db, 'ping', {visibility: 5})
        let msg
        let id

        id = await queue.add('Hello, World!')
        t.ok(id, 'There is an id returned when adding a message.')
        // get something now and it shouldn't be there
        msg = await queue.get()
        t.ok(msg.id, 'Got this message id')
        // ack the message
        id = await queue.ack(msg.ack)
        t.ok(id, 'Received an id when acking this message')
        // ping this message, even though it has been acked
        id = await queue.ping(msg.ack)
            .catch((err) => t.ok(err, 'Error when pinging an acked message'))
        t.ok(!id, 'Received no id when pinging an acked message')

        t.pass('Finished test ok')
        t.end()
    })

    test("ping: check visibility option overrides the queue visibility", async function (t) {
        const queue = new MongoDbQueue(db, 'ping', {visibility: 3})
        let msg
        let id

        id = await queue.add('Hello, World!')
        t.ok(id, 'There is an id returned when adding a message.')
        msg = await queue.get()
        // message should reset in three seconds
        t.ok(msg.id, 'Got a msg.id (sanity check)')
        await timeout(2_000)
        // ping this message so it will be kept alive longer, another 5s instead of 3s
        id = await queue.ping(msg.ack, {visibility: 5})
        t.ok(id, 'Received an id when acking this message')
        // wait 4s so the msg would normally have returns to the queue
        await timeout(4_000)
        msg = await queue.get()
        // messages should not be back yet
        t.ok(!msg, 'No msg received')
        // wait 2s so the msg should have returns to the queue
        await timeout(2_000)
        msg = await queue.get()
        // yes, there should be a message on the queue again
        t.ok(msg.id, 'Got a msg.id (sanity check)')
        await queue.ack(msg.ack)
        msg = await queue.get()
        // no more messages
        t.ok(!msg, 'No msg received')

        t.pass('Finished test ok')
        t.end()
    })

    test("ping: reset tries", async function (t) {
        const queue = new MongoDbQueue(db, 'ping', {visibility: 3})
        let msg
        let id

        id = await queue.add('Hello, World!')
        t.ok(id, 'There is an id returned when adding a message.')
        msg = await queue.get()
        // message should reset in three seconds
        t.ok(msg.id, 'Got a msg.id (sanity check)')
        await timeout(2_000)
        id = await queue.ping(msg.ack, {resetTries: true})
        t.ok(id, 'Received an id when acking this message')
        // wait until the msg has returned to the queue
        await timeout(6_000)
        msg = await queue.get()
        t.equal(msg.tries, 1, 'Tries were reset')
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
