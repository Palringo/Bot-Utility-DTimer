/* eslint-disable */

const DTimer = require('..').DTimer;
const async = require('async');
const assert = require('assert');
const redis = require('ioredis');

// TODO: Failing
describe('Multiple nodes', () => {
    const numNodes = 8;
    let nodes;

    before(() => {
    });

    beforeEach((done) => {
        nodes = [];
        function prepareNode(id, cb) {
            const node = {
                id,
                numRcvd: 0,
            };
            async.series([
                function(next) {
                    node.pub = redis.createClient();
                    node.pub.once('ready', next);
                },
                function(next) {
                    node.sub = redis.createClient();
                    node.sub.once('ready', next);
                },
                function(next) {
                    node.pub.select(9, next);
                },
            ], (err) => {
                if (err) { return void(cb(err)); }
                node.dt = new DTimer(`ch${ id}`, node.pub, node.sub);
                // eslint-disable-next-line no-magic-numbers
                setTimeout(() => { cb(null, node); }, 100);
            });
        }

        async.whilst(
            () => (nodes.length < numNodes),
            (next) => {

                console.log('preparing nodes');
                prepareNode(nodes.length, (err, node) => {
                    if (err) { return void(next(err)); }
                    nodes.push(node);
                    next();
                });
            },
            (err) => {
                if (err) {
                    return done(err);
                }
                nodes[0].pub.flushdb();
                done();
            },
        );
    });

    afterEach(() => {
        nodes.forEach((node) => {
            node.dt = null;
            node.pub.end();
            node.pub = null;
            node.sub.end();
            node.sub = null;
        });
        nodes = [];
    });

    it('MaxEvents 1 - each node receives 1 event', (done) => {
        const evts = [
            { msg: { msg: 'msg0' }, delay: 60 },
            { msg: { msg: 'msg1' }, delay: 60 },
            { msg: { msg: 'msg2' }, delay: 60 },
            { msg: { msg: 'msg3' }, delay: 60 },
            { msg: { msg: 'msg4' }, delay: 60 },
            { msg: { msg: 'msg5' }, delay: 60 },
            { msg: { msg: 'msg6' }, delay: 60 },
            { msg: { msg: 'msg7' }, delay: 60 },
        ];
        let numRcvd = 0;

        // Set up each node to grab 1 event at a time.
        nodes.forEach((node) => {
            node.dt.maxEvents = 1;
        });
        async.series([
            function(next) {
                let numJoined = 0;
                nodes.forEach((node) => {
                    node.dt.join((err) => {
                        if (err) { return void(next(err)); }
                        numJoined++;
                        if (numJoined === nodes.length) {
                            return next();
                        }
                    });
                });
            },
            function(next) {
                const since = Date.now();
                evts.forEach((evt) => {
                    nodes[0].dt.post(evt.msg, evt.delay, (err, evId) => {
                        assert.ifError(err);
                        evt.id = evId;
                        evt.postDelay = Date.now() - since;
                        evt.posted = true;
                    });
                });
                nodes.forEach((node) => {
                    node.dt.on('event', (ev) => {
                        node.numRcvd++;
                        const elapsed = Date.now() - since;
                        evts.forEach((evt) => {
                            if (evt.msg.msg === ev.msg) {
                                numRcvd++;
                                evt.elapsed = elapsed;
                                evt.rcvd = ev;
                                evt.rcvdBy = node.id;
                                evt.order = numRcvd;
                            }
                        });
                    });
                });
                setTimeout(next, 100);
            },
            function(next) {
                nodes[0].pub.llen('dt:ch', (err, reply) => {
                    assert.ifError(err);
                    assert.equal(reply, 8);
                    next();
                });
            },
            function(next) {
                let numLeft = 0;
                nodes.forEach((node) => {
                    node.dt.leave(() => {
                        numLeft ++;
                        if (numLeft === nodes.length) {
                            return next();
                        }
                    });
                });
            },
        ], (err, results) => {
            void(results);
            assert.ifError(err);
            evts.forEach((evt) => {
                assert.ok(evt.posted);
                assert.deepEqual(evt.msg.msg, evt.rcvd.msg);
                assert(evt.elapsed < evt.delay + 200);
                assert(evt.elapsed > evt.delay);
            });
            assert.equal(numRcvd, evts.length);
            nodes.forEach((node) => {
                assert.equal(node.numRcvd, 1);
            });
            done();
        });
    });

    it('MaxEvents 2 - each node receives 0 or 2 events', (done) => {
        const evts = [
            { msg: { msg: 'msg0' }, delay: 60 },
            { msg: { msg: 'msg1' }, delay: 60 },
            { msg: { msg: 'msg2' }, delay: 60 },
            { msg: { msg: 'msg3' }, delay: 60 },
            { msg: { msg: 'msg4' }, delay: 60 },
            { msg: { msg: 'msg5' }, delay: 60 },
            { msg: { msg: 'msg6' }, delay: 60 },
            { msg: { msg: 'msg7' }, delay: 60 },
        ];
        let numRcvd = 0;

        // Set up each node to grab 1 event at a time.
        nodes.forEach((node) => {
            node.dt.maxEvents = 2;
        });
        async.series([
            function(next) {
                let numJoined = 0;
                nodes.forEach((node) => {
                    node.dt.join((err) => {
                        if (err) { return void(next(err)); }
                        numJoined++;
                        if (numJoined === nodes.length) {
                            return next();
                        }
                    });
                });
            },
            function(next) {
                const since = Date.now();
                evts.forEach((evt) => {
                    nodes[0].dt.post(evt.msg, evt.delay, (err, evId) => {
                        assert.ifError(err);
                        evt.id = evId;
                        evt.postDelay = Date.now() - since;
                        evt.posted = true;
                    });
                });
                nodes.forEach((node) => {
                    node.dt.on('event', (ev) => {
                        node.numRcvd++;
                        const elapsed = Date.now() - since;
                        evts.forEach((evt) => {
                            if (evt.msg.msg === ev.msg) {
                                numRcvd++;
                                evt.elapsed = elapsed;
                                evt.rcvd = ev;
                                evt.rcvdBy = node.id;
                                evt.order = numRcvd;
                            }
                        });
                    });
                });
                setTimeout(next, 100);
            },
            function(next) {
                nodes[0].pub.llen('dt:ch', (err, reply) => {
                    assert.ifError(err);
                    assert.equal(reply, 8);
                    next();
                });
            },
            function(next) {
                let numLeft = 0;
                nodes.forEach((node) => {
                    node.dt.leave(() => {
                        numLeft ++;
                        if (numLeft === nodes.length) {
                            return next();
                        }
                    });
                });
            },
        ], (err, results) => {
            void(results);
            assert.ifError(err);
            evts.forEach((evt) => {
                assert.ok(evt.posted);
                assert.deepEqual(evt.msg.msg, evt.rcvd.msg);
                assert(evt.elapsed < evt.delay + 200);
                assert(evt.elapsed > evt.delay);
            });
            assert.equal(numRcvd, evts.length);
            nodes.forEach((node) => {
                if (node.numRcvd > 0) {
                    assert.equal(node.numRcvd, 2);
                }
            });
            done();
        });
    });
});
