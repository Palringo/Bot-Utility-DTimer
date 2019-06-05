
const fs = require('fs');
const util = require('util');
const events = require('events');
const _und = require('underscore');
const debug = require('debug')('dtimer');
const uuid = require('uuid');
const Bluebird = require('bluebird');

Bluebird.promisifyAll(require('redis'));

// defaults
const defaults = {
    ns: 'dtimer',
    maxEvents: 8,

    // Seconds
    readyTimeout: 30,
    confTimeout: 10,
};

// scripts
const scripts = {
    update: {
        script: fs.readFileSync(`${__dirname }/lua/update.lua`, 'utf8'), // eslint-disable-line no-sync
    },
    cancel: {
        script: fs.readFileSync(`${__dirname }/lua/cancel.lua`, 'utf8'), // eslint-disable-line no-sync
    },
    changeDelay: {
        script: fs.readFileSync(`${__dirname }/lua/changeDelay.lua`, 'utf8'), // eslint-disable-line no-sync
    },
};

// Workaround for in-result errors for multi operation.
// This will not be necessary with redis@>=2.0.0.
function throwIfMultiError(results) {
    results.forEach((res) => {
        if (typeof res === 'string' && res.indexOf('ERR') === 0) {
            throw new Error(res);
        }
    });
}

// Redis key name policy
// Global hash: $ns + ':' + 'gl', hash {field-name, value}
//    o lastId
// Channel table    : $ns + ':' + 'chs', hash-set {last-ts, node-id}
// Event table   : $ns + ':' + 'evs', hash {field-name, value}

function DTimer(id, pub, sub, option) {
    const self = this;
    this._timer = null;
    this._option = _und.defaults(option || {}, defaults);
    this._pub = pub;
    if (!this._pub) {
        throw new Error('Redis client (pub) is missing');
    }
    this._sub = sub;
    if (this._sub) {
        this._sub.on('message', this._onSubMessage.bind(this));
        if (typeof id !== 'string' || id.length === 0) {
            throw new Error('The id must be non-empty string');
        }
    } else {
        id = 'post-only'; // eslint-disable-line no-param-reassign
    }
    this._keys = {
        gl: `${self._option.ns}.gl`,
        ch: `${self._option.ns}.ch`,
        ei: `${self._option.ns}.ei`,
        ed: `${self._option.ns}.ed`,
        et: `${self._option.ns}.et`,
    };

    // subscriber channel
    this._id = `${this._keys.ch}.${id}`;
    this._lur = require('lured').create(this._pub, scripts); // eslint-disable-line global-require
    this._lur.load((err) => {
        if (err) {
            debug(`${self._id}: lua loading failed: ${err.name}`);
            self.emit('error', err);
            return;
        }
        debug(`${self._id}: lua loading successful`);
    });
    this._maxEvents = this._option.maxEvents;

    // Define getter 'maxEvents'
    this.__defineGetter__('maxEvents', function() { // eslint-disable-line func-names
        return this._maxEvents;
    });

    // Define setter 'maxEvents'
    this.__defineSetter__('maxEvents', function(num) { // eslint-disable-line func-names
        this._maxEvents = (num > 0) ? num : this._option.maxEvents;
    });
}

util.inherits(DTimer, events.EventEmitter);

DTimer.prototype._onSubMessage = function(chId, msg) { // eslint-disable-line func-names
    void(chId);
    try {
        const o = JSON.parse(msg);
        if (typeof o.interval === 'number') {
            if (this._timer) {
                clearTimeout(this._timer);
            }
            debug(`${this._id}: new interval (1) ${o.interval}`);
            this._timer = setTimeout(this._onTimeout.bind(this), o.interval);
        }
    } catch (e) {
        debug('Malformed message:', msg);
        this.emit('error', e);
    }
};

DTimer.prototype.join = function(cb) { // eslint-disable-line func-names
    const self = this;
    return new Bluebird(((resolve, reject) => { // eslint-disable-line consistent-return
        if (!self._sub) {
            return reject(new Error('Can not join without redis client (sub)'));
        }

        self._sub.subscribe(self._id);
        self._sub.once('subscribe', () => {
            resolve();
        });
    }))
        .then(() => self._redisTime())
        .then((now) => self._pub.multi() // eslint-disable-line promise/no-nesting
            .lrem(self._keys.ch, 0, self._id)
            .lpush(self._keys.ch, self._id)
            .evalsha(
                scripts.update.sha,
                5, // eslint-disable-line no-magic-numbers
                self._keys.gl,
                self._keys.ch,
                self._keys.ei,
                self._keys.ed,
                self._keys.et,
                '',
                now,
                0,
                self._option.confTimeout)
            .execAsync()
            .then((replies) => {
                throwIfMultiError(replies);
                /* istanbul ignore if  */
                if (self._timer) {
                    clearTimeout(self._timer);
                }
                debug(`${self._id}: new interval (2) ${replies[2][1]}`); // eslint-disable-line no-magic-numbers
                self._timer = setTimeout(self._onTimeout.bind(self), replies[2][1]); // eslint-disable-line no-magic-numbers
            }))

    // .nodeify(cb);
        .then(() => cb);
};

DTimer.prototype._redisTime = function() { // eslint-disable-line func-names
    return this._pub.timeAsync()
        .then((result) => result[0] * 1000 + Math.floor(result[1] / 1000)); // eslint-disable-line no-magic-numbers
};

DTimer.prototype.leave = function(cb) { // eslint-disable-line func-names
    if (!this._sub) {
        return Bluebird.reject(new Error('Can not leave without redis client (sub)'))

            // .nodeify(cb);
            .then(() => cb);

    }
    const self = this;

    if (this._timer) {
        clearTimeout(this._timer);
        this._timer = null;
    }

    return this._redisTime()
        .then((now) => self._pub.multi() // eslint-disable-line promise/no-nesting
            .lrem(self._keys.ch, 0, self._id)
            .evalsha(
                scripts.update.sha,
                5, // eslint-disable-line no-magic-numbers
                self._keys.gl,
                self._keys.ch,
                self._keys.ei,
                self._keys.ed,
                self._keys.et,
                '',
                now,
                0,
                self._option.confTimeout)
            .execAsync()
            .then(throwIfMultiError))
        .catch((error) => error)
        .then((result) => { // eslint-disable-line no-unused-vars
            self._sub.unsubscribe(self._id);
            return new Bluebird(((resolve) => {
                self._sub.once('unsubscribe', () => {
                    resolve();
                });
            }));
        })

    // .nodeify(cb);
        .then(() => cb);
};

DTimer.prototype.post = function(ev, delay, cb) { // eslint-disable-line func-names
    const self = this;
    let evId;

    if (typeof delay !== 'number') {
        throw new Error('delay argument must be of type number');
    }

    // Copy event.
    ev = JSON.parse(JSON.stringify(ev)); // eslint-disable-line no-param-reassign

    if (typeof ev !== 'object') {
        throw new Error('event data must be of type object');
    }

    if (ev.hasOwnProperty('id')) {
        if (typeof ev.id !== 'string' || ev.id.length === 0) {
            throw new Error('event ID must be a non-empty string');
        }
    } else {
        ev.id = uuid.v4();
    }
    evId = ev.id; // eslint-disable-line prefer-const

    if (ev.hasOwnProperty('maxRetries')) {
        if (typeof ev.maxRetries !== 'number') {
            throw new Error('maxRetries must be a number');
        }
    } else {
        ev.maxRetries = 0;
    }

    const msg = JSON.stringify(ev);

    return this._redisTime()
        .then((now) => self._pub.multi() // eslint-disable-line promise/no-nesting
            .zadd(self._keys.ei, now + delay, evId)
            .hset(self._keys.ed, evId, msg)
            .evalsha(
                scripts.update.sha,
                5, // eslint-disable-line no-magic-numbers
                self._keys.gl,
                self._keys.ch,
                self._keys.ei,
                self._keys.ed,
                self._keys.et,
                '',
                now,
                0,
                self._option.confTimeout)
            .execAsync()
            .then((results) => {
                throwIfMultiError(results);
                return evId;
            }))

    // .nodeify(cb);
        .then(() => cb);
};

DTimer.prototype.peek = function(evId, cb) { // eslint-disable-line func-names
    const self = this;

    return this._redisTime()
        .then((now) => self._pub.multi() // eslint-disable-line promise/no-nesting
            .zscore(self._keys.ei, evId)
            .hget(self._keys.ed, evId)
            .execAsync()
            .then((results) => {
                throwIfMultiError(results);
                if (results[0] === null || results[1] === null) {
                    return [null, null];
                }
                return [
                    Math.max(parseInt(results[0]) - now, 0),
                    JSON.parse(results[1]),
                ];
            }))

    // .nodeify(cb);
        .then(() => cb);
};

DTimer.prototype.cancel = function(evId, cb) { // eslint-disable-line func-names
    const self = this;

    return this._redisTime()
        .then((now) => self._pub.multi() // eslint-disable-line promise/no-nesting
            .evalsha(
                scripts.cancel.sha,
                2, // eslint-disable-line no-magic-numbers
                self._keys.ei,
                self._keys.ed,
                evId)
            .evalsha(
                scripts.update.sha,
                5, // eslint-disable-line no-magic-numbers
                self._keys.gl,
                self._keys.ch,
                self._keys.ei,
                self._keys.ed,
                self._keys.et,
                '',
                now,
                0,
                self._option.confTimeout)
            .execAsync()
            .then((results) => {
                throwIfMultiError(results);
                return results[0];
            }))

    // .nodeify(cb);
        .then(() => cb);
};

DTimer.prototype.confirm = function(evId, cb) { // eslint-disable-line func-names
    const self = this;

    return this._redisTime()
        .then((now) => self._pub.multi() // eslint-disable-line promise/no-nesting
            .evalsha(

                // reuse cancel.lua script
                scripts.cancel.sha,
                2, // eslint-disable-line no-magic-numbers
                self._keys.et,
                self._keys.ed,
                evId)
            .evalsha(
                scripts.update.sha,
                5, // eslint-disable-line no-magic-numbers
                self._keys.gl,
                self._keys.ch,
                self._keys.ei,
                self._keys.ed,
                self._keys.et,
                '',
                now,
                0,
                self._option.confTimeout)
            .execAsync()
            .then((results) => {
                throwIfMultiError(results);
                return results[0];
            }))

    // .nodeify(cb);
        .then(() => cb);
};

DTimer.prototype.changeDelay = function(evId, delay, cb) { // eslint-disable-line func-names
    const self = this;

    if (typeof delay !== 'number') {
        throw new Error('delay argument must be of type number');
    }

    return this._redisTime()
        .then((now) => self._pub.multi() // eslint-disable-line promise/no-nesting
            .evalsha(
                scripts.changeDelay.sha,
                1,
                self._keys.ei,
                evId,
                now + delay)
            .evalsha(
                scripts.update.sha,
                5, // eslint-disable-line no-magic-numbers
                self._keys.gl,
                self._keys.ch,
                self._keys.ei,
                self._keys.ed,
                self._keys.et,
                '',
                now,
                0,
                self._option.confTimeout)
            .execAsync()
            .then((results) => {
                throwIfMultiError(results);
                return results[0];
            }))

    // .nodeify(cb);
        .then(() => cb);
};

DTimer.prototype._onTimeout = function() { // eslint-disable-line func-names
    const self = this;
    this._timer = null;
    this._redisTime() // eslint-disable-line promise/catch-or-return
        .then((now) => {
            let interval;
            return self._pub.evalshaAsync( // eslint-disable-line promise/no-nesting
                scripts.update.sha,
                5, // eslint-disable-line no-magic-numbers
                self._keys.gl,
                self._keys.ch,
                self._keys.ei,
                self._keys.ed,
                self._keys.et,
                self._id,
                now,
                self._maxEvents,
                self._option.confTimeout)
                .then((replies) => {
                    interval = replies[1];
                    if (replies[0].length > 0) {
                        replies[0].forEach((sev) => {
                            let ev;
                            try {
                                ev = JSON.parse(sev);
                            } catch (e) {
                                debug(`${self._id}: fail to parse event. ${JSON.stringify(e)}`);
                                return;
                            }
                            self.emit('event', ev);
                        });
                    }
                }, (err) => {
                    interval = 3000; // eslint-disable-line no-magic-numbers
                    debug(`${self._id}: update failed: ${err.name}`);
                })
                .catch((error) => error)
                .then((result) => { // eslint-disable-line no-unused-vars
                    if (!self._timer) {
                        debug(`${self._id}: new interval (3) ${interval}`);
                        self._timer = setTimeout(self._onTimeout.bind(self), interval);
                    }
                });
        }, (err) => {
            self.emit('error', err);
        });
};

DTimer.prototype.upcoming = function(option, cb) { // eslint-disable-line func-names
    const self = this;
    const defaults = { // eslint-disable-line no-shadow
        offset: -1,

        // +inf
        duration: -1,
        limit: -1,
    };
    let _option;

    if (typeof option !== 'object') { // eslint-disable-line no-negated-condition
        cb = option; // eslint-disable-line no-param-reassign
        _option = defaults;
    } else {
        _option = _und.defaults(option, defaults);
    }

    return this._redisTime()
        .then((now) => {
            let args = [self._keys.ei];
            let offset = 0;
            if (typeof _option.offset !== 'number' || _option.offset < 0) {
                args.push(0);
            } else {
                args.push(now + _option.offset);
                offset = _option.offset;
            }
            if (typeof _option.duration !== 'number' || _option.duration < 0) {
                args.push('+inf');
            } else {
                args.push(now + offset + _option.duration);
            }
            args.push('WITHSCORES');
            if (typeof _option.limit === 'number' && _option.limit > 0) {
                args.push('LIMIT');
                args.push(0);
                args.push(_option.limit);
            }
            debug(`upcoming args: ${JSON.stringify(args)}`);

            return self._pub.zrangebyscoreAsync(args) // eslint-disable-line promise/no-nesting
                .then((results) => {
                    if (results.length === 0) {
                        return {};
                    }

                    const out = [];
                    args = [self._keys.ed];
                    for (let i = 0; i < results.length; i = i + 2) { // eslint-disable-line no-magic-numbers
                        out.push({ expireAt: parseInt(results[i + 1]), id: results[i] });
                        args.push(results[i]);
                    }

                    return self._pub.hmgetAsync(args) // eslint-disable-line promise/no-nesting
                        .then((results) => { // eslint-disable-line no-shadow
                            const outObj = {};
                            results.forEach((evStr, index) => {
                                /* istanbul ignore if  */
                                if (!evStr) {
                                    return;
                                }
                                /* istanbul ignore next  */
                                try {
                                    var event = JSON.parse(evStr); // eslint-disable-line
                                } catch (e) {
                                    debug(`${self._id}: fail to parse event. ${JSON.stringify(e)}`);
                                    return;
                                }
                                outObj[out[index].id] = { expireAt: out[index].expireAt, event }; // eslint-disable-line block-scoped-var
                            });
                            return outObj;
                        });
                });
        })

    // .nodeify(cb);
        .then(() => cb);
};

module.exports.DTimer = DTimer;
