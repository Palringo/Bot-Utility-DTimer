/* eslint-disable consistent-return */
/* eslint-disable promise/no-nesting */
const fs = require('fs');
const util = require('util');
const events = require('events');
const _und = require('underscore');
const uuidv4 = require('uuid').v4;
require('ioredis');

//
const MILLISECOND = 1000;
const THREE_SECONDS = 3000;

// defaults
const defaults = {
    ns: 'dt',
    maxEvents: 8,
    readyTimeout: 30, // in seconds
    confTimeout: 10, // in seconds
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
        const error = _und.first(res);
        if (error !== null) { throw new Error(error); }
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

    this._pub.defineCommand('update', {
        numberOfKeys: 5,
        lua: scripts.update.script,
    });

    this._pub.defineCommand('cancel', {
        numberOfKeys: 2,
        lua: scripts.cancel.script,
    });

    this._pub.defineCommand('changeDelay', {
        numberOfKeys: 1,
        lua: scripts.changeDelay.script,
    });

    this._sub = sub;
    if (this._sub) {
        this._sub.on('message', this._onSubMessage.bind(this));
        if (typeof id !== 'string' || id.length === 0) {
            throw new Error('The id must be non-empty string');
        }
    } else {
        // eslint-disable-next-line no-param-reassign
        id = 'post-only';
    }
    this._keys = {
        gl: `${self._option.ns }:gl`,
        ch: `${self._option.ns }:ch`,
        ei: `${self._option.ns }:ei`,
        ed: `${self._option.ns }:ed`,
        et: `${self._option.ns }:et`,
    };
    this._id = `${this._keys.ch }:${ id}`; // subscriber channel
    this._lur = require('lured').create(this._pub, scripts); // eslint-disable-line global-require
    this._lur.load((err) => {
        if (err) {
            self.emit('error', err);
            return;
        }
        self.emit('ready');
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

            this._timer = setTimeout(this._onTimeout.bind(this), o.interval);
        }
    } catch (error) {

        this.emit('error', error);
    }
};

DTimer.prototype.join = function() { // eslint-disable-line func-names
    const self = this;
    return new Promise(((resolve, reject) => {
        if (!self._sub) {
            return reject(new Error('Can not join without redis client (sub)'));
        }

        self._sub.subscribe(self._id, (err) => {
            if (err) { return reject(err); }
            resolve();
        });
    })).then(() => self._redisTime()).then((now) => self._pub.multi()
        .lrem(self._keys.ch, 0, self._id)
        .lpush(self._keys.ch, self._id)
        .update(
            self._keys.gl,
            self._keys.ch,
            self._keys.ei,
            self._keys.ed,
            self._keys.et,
            '',
            now,
            0,
            self._option.confTimeout,
        ).exec().then((replies) => {
            throwIfMultiError(replies);
            /* istanbul ignore if  */
            if (self._timer) {
                clearTimeout(self._timer);
            }

            self._timer = setTimeout(self._onTimeout.bind(self), replies[2][1][1]);
        }));
};

DTimer.prototype._redisTime = function() { // eslint-disable-line func-names

    return Promise.resolve(this._pub.time().then((result) => result[0] * MILLISECOND + Math.floor(result[1] / MILLISECOND)));
};

DTimer.prototype.leave = function() { // eslint-disable-line func-names
    if (!this._sub) {
        return Promise.reject(new Error('Can not leave without redis client (sub)'));
    }
    const self = this;

    if (this._timer) {
        clearTimeout(this._timer);
        this._timer = null;
    }

    return this._redisTime()
        .then((now) => self._pub.multi()
            .lrem(self._keys.ch, 0, self._id)
            .update(
                self._keys.gl,
                self._keys.ch,
                self._keys.ei,
                self._keys.ed,
                self._keys.et,
                '',
                now,
                0,
                self._option.confTimeout)
            .exec()
            .then(throwIfMultiError))
        .finally(() => new Promise(((resolve) => {
            self._sub.unsubscribe(self._id, () => {
                resolve();
            });
        })));
};

DTimer.prototype.post = function(ev, delay) { // eslint-disable-line func-names
    const self = this;

    if (typeof delay !== 'number') {
        throw new Error('delay argument must be of type number');
    }

    // Copy event.
    // eslint-disable-next-line no-param-reassign
    ev = JSON.parse(JSON.stringify(ev));

    if (typeof ev !== 'object') {
        throw new Error('event data must be of type object');
    }

    // eslint-disable-next-line no-prototype-builtins
    if (ev.hasOwnProperty('id')) {
        if (typeof ev.id !== 'string' || ev.id.length === 0) {
            throw new Error('event ID must be a non-empty string');
        }
    } else {
        ev.id = uuidv4();
    }
    const evId = ev.id;

    // eslint-disable-next-line no-prototype-builtins
    if (ev.hasOwnProperty('maxRetries')) {
        if (typeof ev.maxRetries !== 'number') {
            throw new Error('maxRetries must be a number');
        }
    } else {
        ev.maxRetries = 0;
    }

    const msg = JSON.stringify(ev);

    return this._redisTime()
        .then((now) => self._pub.multi()
            .zadd(self._keys.ei, now + delay, evId)
            .hset(self._keys.ed, evId, msg)
            .update(
                self._keys.gl,
                self._keys.ch,
                self._keys.ei,
                self._keys.ed,
                self._keys.et,
                '',
                now,
                0,
                self._option.confTimeout)
            .exec()
            .then((results) => {
                throwIfMultiError(results);
                return evId;
            }));
};

DTimer.prototype.peek = function(evId) { // eslint-disable-line func-names
    const self = this;

    return this._redisTime()
        .then((now) => self._pub.multi()
            .zscore(self._keys.ei, evId)
            .hget(self._keys.ed, evId)
            .exec()
            .then((results) => {

                throwIfMultiError(results);

                if (results[0][1] === null || results[1][1] === null) {
                    return null;
                }

                return {
                    remaining: Math.max(parseInt(results[0][1]) - now, 0),
                    timer: JSON.parse(results[1][1]),
                };
            }));
};

DTimer.prototype.cancel = function(evId) { // eslint-disable-line func-names
    const self = this;

    return this._redisTime()
        .then((now) => self._pub.multi()
            .cancel(self._keys.ei, self._keys.ed, evId)
            .update(
                self._keys.gl,
                self._keys.ch,
                self._keys.ei,
                self._keys.ed,
                self._keys.et,
                '',
                now,
                0,
                self._option.confTimeout)
            .exec()
            .then((results) => {
                throwIfMultiError(results);
                return results[0][1];
            }));
};

DTimer.prototype.confirm = function(evId) { // eslint-disable-line func-names
    const self = this;

    return this._redisTime()
        .then((now) => self._pub.multi()
            .cancel(self._keys.et, self._keys.ed, evId)
            .update(
                self._keys.gl,
                self._keys.ch,
                self._keys.ei,
                self._keys.ed,
                self._keys.et,
                '',
                now,
                0,
                self._option.confTimeout)
            .exec()
            .then((results) => {
                throwIfMultiError(results);
                return results[0][1];
            }));
};

DTimer.prototype.changeDelay = function(evId, delay) { // eslint-disable-line func-names
    const self = this;

    if (typeof delay !== 'number') {
        throw new Error('delay argument must be of type number');
    }

    return this._redisTime()
        .then((now) => self._pub.multi()
            .changeDelay(self._keys.ei, evId, now + delay)
            .update(
                self._keys.gl,
                self._keys.ch,
                self._keys.ei,
                self._keys.ed,
                self._keys.et,
                '',
                now,
                0,
                self._option.confTimeout)
            .exec()
            .then((results) => {
                throwIfMultiError(results);
                return results[0][1];
            }));
};

DTimer.prototype._onTimeout = function() { // eslint-disable-line func-names
    const self = this;
    this._timer = null;
    return this._redisTime()
        .then((now) => {
            let interval;
            return self._pub.update(
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
                            } catch (error) {
                                return;
                            }
                            self.emit('event', ev);
                        });
                    }
                }).catch(() => {
                    interval = THREE_SECONDS;
                }).finally(() => {
                    if (!self._timer) {
                        self._timer = setTimeout(self._onTimeout.bind(self), interval);
                    }
                });
        }, (err) => {
            self.emit('error', err);
        });
};

DTimer.prototype.upcoming = function(option) { // eslint-disable-line func-names
    const self = this;
    const options = {
        offset: -1,
        duration: -1, // +inf
        limit: -1, // +inf
    };
    let _option;

    if (typeof option === 'object') {
        _option = _und.defaults(option, options);
    } else {
        _option = options;
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

            return self._pub.zrangebyscore(args)
                .then((zrangeResults) => {
                    if (zrangeResults.length === 0) {
                        return {};
                    }

                    const out = [];
                    args = [self._keys.ed];
                    for (let i = 0; i < zrangeResults.length; i = i + 2) { // eslint-disable-line no-magic-numbers
                        out.push({ expireAt: parseInt(zrangeResults[i + 1]), id: zrangeResults[i] });
                        args.push(zrangeResults[i]);
                    }

                    return self._pub.hmget(args)
                        .then((hmgetResults) => {
                            const outObj = {};
                            hmgetResults.forEach((evStr, index) => {
                                /* istanbul ignore if  */
                                if (!evStr) {
                                    return;
                                }

                                outObj[out[index].id] = { expireAt: out[index].expireAt, event: JSON.parse(evStr) };

                            });
                            return outObj;
                        });
                });
        });
};

module.exports.DTimer = DTimer;
