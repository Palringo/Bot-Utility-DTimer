/* eslint-disable */

const DTimer = require('..').DTimer;
const async = require('async');
const assert = require('assert');
const sinon = require('sinon');
const redis = require('ioredis');

describe('Error tests', () => {
    let pub = null;
    let sub = null;
    let dt = null;
    let sandbox;

    before(() => {
        sandbox = sinon.createSandbox();
    });

    beforeEach((done) => {

        let conns = 0;

        pub = redis.createClient();
        pub.once('ready', () => { conns++; });
        sub = redis.createClient();
        sub.once('ready', () => { conns++; });

        const createDTimerOnReady = async() => {

            if (conns >= 2) {
                dt = new DTimer('ch1', pub, sub);

                setTimeout(done, 100);

                return;
            }

            setTimeout(createDTimerOnReady, 100);

        };

        createDTimerOnReady();
    });
    afterEach(() => {
        dt = null;
        pub.end();
        pub = null;
        sub.end();
        sub = null;
        sandbox.restore();
    });

    it('#join', () => {
        sandbox.stub(pub, 'time').callsFake(() => Promise.reject(new Error('fail error')));

        dt.join().catch((err) => {
            assert.ok(err);
        });
    });

    it('#leave', () => {
        sandbox.stub(pub, 'time').callsFake(() => Promise.reject(new Error('fail error')));

        dt.leave().catch((err) => {
            assert.ok(err);
        });
    });

    it('#post', () => {
        sandbox.stub(pub, 'time').callsFake(() => Promise.reject(new Error('fail error')));

        dt.post({}, 100).catch((err) => {
            assert.ok(err);
        });
    });

    it('#cancel', () => {
        sandbox.stub(pub, 'time').callsFake(() => Promise.reject(new Error('fail error')));

        dt.cancel(3).catch((err) => {
            assert.ok(err);
        });
    });

    it('#cancel - multi error', () => {
        sandbox.stub(pub, 'multi').callsFake(() => {
            const multi = {
                evalsha() { return multi; },
                execAsync() {
                    return Promise.reject(new Error('fake error'));
                },
            };
            return multi;
        });

        dt.cancel('myEvent').catch((err) => {
            assert.ok(err);
            assert.equal(err.name, 'Error');
        });
    });

    it('#confirm - error with time command', () => {
        sandbox.stub(pub, 'time').callsFake(() => Promise.reject(new Error('fail error')));

        dt.confirm('myEvent').catch((err) => {
            assert.ok(err);
        });
    });

    it('#confirm - multi error', () => {
        sandbox.stub(pub, 'multi').callsFake(() => {
            const multi = {
                evalsha() { return multi; },
                execAsync() {
                    return Promise.reject(new Error('fake error'));
                },
            };
            return multi;
        });

        dt.confirm('myEvent').catch((err) => {
            assert.ok(err);
            assert.equal(err.name, 'Error');
        });
    });

    it('#changeDelay - error with time command', () => {
        sandbox.stub(pub, 'time').callsFake(() => Promise.reject(new Error('fail error')));

        dt.changeDelay('myEvent', 1000).catch((err) => {
            assert.ok(err);
        });
    });

    it('#changeDelay - multi error', () => {
        sandbox.stub(pub, 'multi').callsFake(() => {
            const multi = {
                evalsha() { return multi; },
                execAsync() {
                    return Promise.reject(new Error('fake error'));
                },
            };
            return multi;
        });

        dt.changeDelay('myEvent', 1000).catch((err) => {
            assert.ok(err);
            assert.equal(err.name, 'Error');
        });
    });

    it('#_onTimeout', () => {
        sandbox.stub(pub, 'time').callsFake(() => Promise.reject(new Error('fail error')));
        dt.on('error', (err) => {
            assert.ok(err);
        });
        dt._onTimeout();
    });

    it('#join - multi error', () => {
        sandbox.stub(pub, 'multi').callsFake(() => {
            const m = {
                lrem() { return this; },
                lpush() { return this; },
                zadd() { return this; },
                zrem() { return this; },
                hset() { return this; },
                hdel() { return this; },
                evalsha() { return this; },
                execAsync() {
                    return Promise.reject(new Error('fake err'));
                },
            };
            return m;
        });

        dt.join().catch((err) => {
            assert.ok(err);
            assert.equal(err.name, 'Error');
        });
    });

    it('#post - multi error', () => {
        sandbox.stub(pub, 'multi').callsFake(() => {
            const m = {
                lrem() { return this; },
                lpush() { return this; },
                zadd() { return this; },
                zrem() { return this; },
                hset() { return this; },
                hdel() { return this; },
                evalsha() { return this; },
                execAsync() {
                    return Promise.reject(new Error('fake err'));
                },
            };
            return m;
        });

        dt.post({}, 200).catch((err) => {
            assert.ok(err);
            assert.equal(err.name, 'Error');
        });
    });

    it('#post - multi (in-result) error', () => {
        sandbox.stub(pub, 'multi').callsFake(() => {
            const m = {
                lrem() { return this; },
                lpush() { return this; },
                zadd() { return this; },
                zrem() { return this; },
                hset() { return this; },
                hdel() { return this; },
                hmget() { return this; },
                evalsha() { return this; },
                execAsync() {
                    return Promise.resolve(['ERR fakeed', 1, 1]);
                },
            };
            return m;
        });

        dt.post({}, 200).catch((err) => {
            assert.ok(err);
            assert.equal(err.name, 'Error');
        });
    });

    describe('#upcoming', () => {
        beforeEach(() => {
            dt.join();
        });

        it('force _redisTime return error', () => {
            sandbox.stub(dt, '_redisTime').callsFake((c) => {
                void(c);
                return Promise.reject(new Error('fake error'));
            });
            dt.upcoming().catch((err) => {
                assert.ok(err);
            });
        });

        it('force _pub.zrangebyscore return error', () => {
            sandbox.stub(dt._pub, 'zrangebyscore').callsFake((args) => {
                void(args);
                return Promise.reject(new Error('fake error'));
            });
            dt.upcoming().catch((err) => {
                assert.ok(err);
            });
        });

        // TODO: Failing
        it('force _pub.hmget return error', (done) => {
            sandbox.stub(dt._pub, 'multi').callsFake(() => {
                const m = {
                    lrem() { return this; },
                    lpush() { return this; },
                    zadd() { return this; },
                    zrem() { return this; },
                    hset() { return this; },
                    hdel() { return this; },
                    hmget() { return this; },
                    evalsha() { return this; },
                    execAsync() {
                        return Promise.resolve(['ERR fakeed', 1, 1]);
                    },
                };
                return m;
            });

            try {
                async.series([
                    function() { // eslint-disable-line func-names

                        dt.post({ msg: 'bye' }, 1000);
                    },
                    function(next) { // eslint-disable-line func-names
                        sandbox.stub(dt._pub, 'hmget').callsFake((args) => {
                            void(args);
                            return Promise.reject(new Error('fake error'));
                        });

                        dt.upcoming((err) => {
                            assert.ok(err);
                            next();
                        });
                    },
                    function(next) { // eslint-disable-line func-names
                        dt.leave(() => {
                            next();
                        });
                    },
                ], done());
            } catch (error) {
                console.log(error);
                done();
            }
        });
    });
});
