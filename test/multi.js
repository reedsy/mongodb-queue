const test = require('tape')

const setup = require('./setup.js')
const MongoDbQueue = require('../').default

const total = 250

setup().then(({client, db}) => {

    test('multi: add ' + total + ' messages, get ' + total + ' back', async function (t) {
        const queue = new MongoDbQueue(db, 'multi')
        const msgs = []

        for (let i = 0; i < total; i++) await queue.add('no=' + i)
        t.pass('All ' + total + ' messages sent to MongoDB')

        async function getOne() {
            const msg = await queue.get()
            msgs.push(msg)
            if (msgs.length !== total) return getOne()
            t.pass('Received all ' + total + ' messages')
        }
        await getOne()

        await Promise.all(
            msgs.map((msg) => queue.ack(msg.ack))
        )

        t.pass('Acked all ' + total + ' messages')

        t.end()
    })

    test('client.close()', function (t) {
        t.pass('client.close()')
        client.close()
        t.end()
    })

})
