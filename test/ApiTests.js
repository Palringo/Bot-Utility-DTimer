/* eslint-disable */

const DTimer = require('..').DTimer;
const assert = require('assert');
const sinon = require('sinon');
const redis = require('ioredis');

describe('ApiTests', () => {
    let sandbox = sinon.createSandbox();
    let clock;

    before(() => {
        sandbox = sinon.createSandbox();
    });

    beforeEach(() => {
        clock = null;
    });

    afterEach(() => {
        sandbox.restore();
        if (clock) {
            clock.restore();
        }
    });

    it('The pub should not be null', () => {
        assert.throws(() => {
            const dt = new DTimer('me', null);
            void(dt);
        },
        (err) => (err instanceof Error),
        'unexpected error',
        );
    });

    it('When sub is present, id must be non-empty string', () => {
        const pub = redis.createClient();
        const sub = redis.createClient();
        function shouldThrow(id) {
            assert.throws(() => {
                const dt = new DTimer(id, pub, sub);
                void(dt);
            },
            (err) => (err instanceof Error),
            'unexpected error',
            );
        }

        // Should not be undefined
        shouldThrow(void(0));

        // Should not be null
        shouldThrow(null);

        // Should not be a number
        shouldThrow(1);

        // Should not be empty string
        shouldThrow('');
    });

    it('maxEvents - defaults to 8', () => {
        const pub = redis.createClient();
        const dt = new DTimer('me', pub, null);
        assert.strictEqual(dt.maxEvents, 8);
    });
    it('maxEvents - use option to change default', () => {
        const pub = redis.createClient();
        const dt = new DTimer('me', pub, null, { maxEvents: 4 });
        assert.strictEqual(dt.maxEvents, 4);
    });
    it('maxEvents - reset to default by setting with 0', () => {
        const pub = redis.createClient();
        const dt = new DTimer('me', pub, null);
        assert.strictEqual(dt.maxEvents, 8);
        dt.maxEvents = 3;
        assert.strictEqual(dt.maxEvents, 3);
        dt.maxEvents = 0;
        assert.strictEqual(dt.maxEvents, 8);
        dt.maxEvents = 12;
        assert.strictEqual(dt.maxEvents, 12);
        dt.maxEvents = -1;
        assert.strictEqual(dt.maxEvents, 8);
    });

    it('emits error when lured.load() fails', (done) => {
        // eslint-disable-next-line global-require
        sandbox.stub(require('lured'), 'create').callsFake(() => ({
            on() {},
            load(cb) {
                process.nextTick(() => {
                    cb(new Error('fake exception'));
                });
            },
        }));
        const pub = redis.createClient();
        const dt = new DTimer('me', pub, null);
        dt.on('error', (err) => {
            assert(err instanceof Error);
            done();
        });
    });

    it('Detect malformed message on subscribe', (done) => {
        sandbox.stub(require('lured'), 'create').callsFake(() => ({
            on() {},
            load(cb) { process.nextTick(cb); },
        }));
        const pub = redis.createClient();
        const dt = new DTimer('me', pub, null);
        dt.on('error', (err) => {
            assert(err instanceof Error);
            done();
        });
        dt._onSubMessage('me', '{bad');
    });

    it('Ignore invalid interval in pubsub message', () => {
        sandbox.stub(require('lured'), 'create').callsFake(() => ({
            on() {},
            load(cb) { process.nextTick(cb); },
        }));
        const pub = redis.createClient();
        const dt = new DTimer('me', pub, null);
        dt.on('error', (err) => {
            assert.ifError(err);
        });
        dt._onSubMessage('me', '{"interval":true}');
        assert.ok(!dt._timer);
    });

    it('join() should fail without sub', () => {
        sandbox.stub(require('lured'), 'create').callsFake(() => ({
            on() {},
            load(cb) { process.nextTick(cb); },
        }));
        const pub = redis.createClient();
        const dt = new DTimer('me', pub, null);
        dt.join()
            .then(() => {
                assert.fail('join() was successful');
            })
            .catch((reject) => {
                assert.ok(reject);
            });
    });

    it('leave() should fail without sub', () => {
        sandbox.stub(require('lured'), 'create').callsFake(() => ({
            on() {},
            load(cb) { process.nextTick(cb); },
        }));
        const pub = redis.createClient();
        const dt = new DTimer('me', pub, null);
        dt.leave()
            .then(() => {
                assert.fail('leave() was successful');
            })
            .catch((reject) => {
                assert.ok(reject);
            });
    });

    it('Attempts to post non-object event should throw', () => {
        sandbox.stub(require('lured'), 'create').callsFake(() => ({
            on() {},
            load(cb) { process.nextTick(cb); },
            state: 3,
        }));
        const pub = redis.createClient();
        const dt = new DTimer('me', pub, null);
        assert.throws(() => {
            dt.post('please throw', 1000, () => {});
        },
        (err) => (err instanceof Error),
        'unexpected error',
        );
    });

    it('Attempts to post with invalid event ID should throw', () => {
        sandbox.stub(require('lured'), 'create').callsFake(() => ({
            on() {},
            load(cb) { process.nextTick(cb); },
            state: 3,
        }));
        const pub = redis.createClient();
        const dt = new DTimer('me', pub, null);
        assert.throws(() => {
            dt.post({ id: 666 /* bad*/ }, 1000, () => {});
        }, Error);
    });

    it('Attempts to post with invalid maxRetries should throw', () => {
        sandbox.stub(require('lured'), 'create').callsFake(() => ({
            on() {},
            load(cb) { process.nextTick(cb); },
            state: 3,
        }));
        const pub = redis.createClient();
        const dt = new DTimer('me', pub, null);
        assert.throws(() => {
            dt.post({ id: 'should throw', maxRetries: true /* bad*/ }, 1000, () => {});
        }, Error);
    });

    it('Attempts to post with delay of type string should throw', () => {
        sandbox.stub(require('lured'), 'create').callsFake(() => ({
            on() {},
            load(cb) { process.nextTick(cb); },
            state: 3,
        }));
        const pub = redis.createClient();
        const dt = new DTimer('me', pub, null);
        assert.throws(() => {
            dt.post({ id: 'should throw', maxRetries: 5 }, '1000' /* bad*/, () => {});
        }, Error);
    });

    it('Attempts to changeDelay with delay of type string should throw', () => {
        sandbox.stub(require('lured'), 'create').callsFake(() => ({
            on() {},
            load(cb) { process.nextTick(cb); },
            state: 3,
        }));
        const pub = redis.createClient();
        const dt = new DTimer('me', pub, null);
        assert.throws(() => {
            dt.changeDelay({ id: 'should throw', maxRetries: 5 }, '1000' /* bad*/, () => {});
        }, Error);
    });
});
