const test = require('tape')

const setup = require('./setup.js')
const MongoDbQueue = require('../')
const {timeout} = require('./_timeout.js')

setup().then(({client, db}) => {

    test('delay: check messages on this queue are returned after the delay', async function (t) {
        const queue = new MongoDbQueue(db, 'delay', {delay: 3})
        let id
        let msg

        id = await queue.add('Hello, World!')
        t.ok(id, 'There is an id returned when adding a message.')
        // get something now and it shouldn't be there
        msg = await queue.get()
        t.ok(!msg, 'No msg received')
        await timeout(4_000)
        // get something now and it SHOULD be there
        msg = await queue.get()
        t.ok(msg.id, 'Got a message id now that the message delay has passed')
        await queue.ack(msg.ack)
        msg = await queue.get()
        // no more messages
        t.ok(!msg, 'No more messages')
        t.pass('Finished test ok')
        t.end()
    })

    test('delay: check an individual message delay overrides the queue delay', async function (t) {
        const queue = new MongoDbQueue(db, 'delay')
        let id
        let msg

        id = await queue.add('I am delayed by 3 seconds', {delay: 3})
        t.ok(id, 'There is an id returned when adding a message.')
        // get something now and it shouldn't be there
        msg = await queue.get()
        t.ok(!msg, 'No msg received')
        await timeout(4_000)
        // get something now and it SHOULD be there
        msg = await queue.get()
        t.ok(msg.id, 'Got a message id now that the message delay has passed')
        await queue.ack(msg.ack)
        msg = await queue.get()
        // no more messages
        t.ok(!msg, 'No more messages')

        t.pass('Finished test ok')
        t.end()
    })

    test('client.close()', function (t) {
        t.pass('client.close()')
        client.close()
        t.end()
    })

})
