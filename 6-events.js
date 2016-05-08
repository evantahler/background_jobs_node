var nodemailer   = require('nodemailer');
var EventEmitter = require('events').EventEmitter;
var redis        = require('ioredis');
var async        = require('async');
var util         = require('util');
var http         = require('http');
var fs           = require('fs');

var httpPort = process.env.PORT || 8080;
var httpHost = process.env.HOST || '127.0.0.1';

var connectionDetails = {
  host:      "127.0.0.1",
  password:  "",
  port:      6379,
  database:  0,
};

var transporter = nodemailer.createTransport({
  service: 'gmail',
  auth: {
    user: require('./.emailUsername'),
    pass: require('./.emailPassword')
  }
});

////////////
// EVENTR //
////////////

var eventr = function(name, connectionDetails, handlers){
  this.redis = new redis(connectionDetails);
  this.name = name;
  this.prefix = 'eventr:';
  this.partitionSizeInSeconds = 60;
  this.handlers = handlers;
  this.sleepTime = 1000 * 5;

  var lua = fs.readFileSync(__dirname + '/6-lua.lua').toString();
  this.redis.defineCommand('getAndIncr', {
    numberOfKeys: 0,
     lua: lua
  });
};

util.inherits(eventr, EventEmitter);

eventr.prototype.write = function(payload, callback){
  var self = this;
  var data = JSON.stringify(payload);
  var partition = self.nowPartition();
  var key = self.prefix + 'partitions:' + partition;
  self.ensurePartition(partition, function(error){
    if(error){ return callback(error); }
    self.redis.rpush(key, data, callback);
  });
};

eventr.prototype.nowPartition = function(){
  var self = this;
  return Math.floor( new Date().getTime() / 1000 / self.partitionSizeInSeconds );
}

eventr.prototype.firstPartition = function(callback){
  var self = this;
  self.redis.zrange(self.prefix + 'partitions', 0, 0, function(error, partitions){
    if(error){ return callback(error); }
    return callback(null, partitions[0]);
  });
}

eventr.prototype.nextPartition = function(partition, callback){
  var self = this;
  self.redis.zrank(self.prefix + 'partitions', partition, function(error, position){
    if(error){ return callback(error); }
    self.redis.zrange(self.prefix + 'partitions', (position + 1), (position + 1), function(error, partitions){
      if(error){ return callback(error); }
      return callback(null, partitions[0]);
    });
  });
}

eventr.prototype.ensurePartition = function(partition, callback){
  var self = this;
  self.redis.zadd(self.prefix + 'partitions', partition, partition, callback);
}

eventr.prototype.work = function(){
  var self = this;
  var jobs = [];
  self.next(function(error, partition, event){
    if(error){ self.emit('error', error); }
    if(!event){
      self.emit('pause', partition);
      return setTimeout(function(){
        self.work.call(self);
      }, self.sleepTime);
    }else{
      self.emit('event', partition, event);
      self.handlers.forEach(function(handler){
        jobs.push(function(next){
          handler.perform(event, function(error, result){
            if(error){ self.emit('error', handler.name, error); }
            else{ self.emit('success', handler.name, result); }
            next();
          });
        });
      });

      async.series(jobs, function(){
        self.work.call(self);
      });
    }
  });
}

eventr.prototype.next = function(callback){
  var self = this;
  var jobs = [];

  var partition;
  var event;

  // what partition are we working on
  jobs.push(function(next){
    self.redis.hget(self.prefix + 'client_partitions', self.name, function(error, p){
      if(error){ return next(error); }
      if(!p){
        self.firstPartition(function(error, p){
          if(error){ return next(error); }
          if(p){ partition = p; }
          self.redis.hset(self.prefix + 'client_partitions', self.name, partition, next);
        })
      }else{
        partition = p;
        return next();
      }
    });
  });

  // get the next key in this partition and slide the cursor atomically
  jobs.push(function(next){
    if(!partition){ return next(); }
    self.emit('poll', partition);
    self.redis.getAndIncr(self.name, partition, function(error, e){
      if(error){ return next(error); }
      if(e){ event = JSON.parse(e); }
      return next();
    });
  });

  // if we got no event, and we are sufffiecently in the future, try the next partition
  jobs.push(function(next){
    if(!partition){ return next(); }
    if(event){ return next(); }
    self.nextPartition(partition, function(error, nextPartition){
      if(nextPartition){
        self.redis.hset(self.prefix + 'client_partitions', self.name, nextPartition, next);
      }else{
        next();
      }
    });
  });

  async.series(jobs, function(error){
    if(error){ return callback(error); }
    return callback(null, partition, event);
  });
};

////////////////
// WEB SERVER //
////////////////

var server = function(req, res){
  var urlParts = req.url.split('/');
  var email    = {
    to:      decodeURI(urlParts[1]),
    subject: decodeURI(urlParts[2]),
    text:    decodeURI(urlParts[3]),
  };

  producer.write({
    eventName: 'emailEvent',
    email: email
  }, function(error){
    if(error){ console.log(error) }
    console.log('email :' + JSON.stringify(email));
    var response = {email: email};
    res.writeHead(200, {'Content-Type': 'application/json'});
    res.end(JSON.stringify(response, null, 2));
  });
};

var producer = new eventr('webApp', connectionDetails);
http.createServer(server).listen(httpPort, httpHost);
console.log('Server running at ' + httpHost + ':' + httpPort);
console.log('send an email and message to /TO_ADDRESS/SUBJECT/YOUR_MESSAGE');

//////////////
// CONSUMER //
//////////////

var handlers = [
  {
    name: 'email handler',
    perform: function(event, callback){
      if(event.eventName === 'emailEvent'){
        var email = {
          from:    require('./.emailUsername'),
          to:      event.email.to,
          subject: event.email.subject,
          text:    event.email.text,
        };

        transporter.sendMail(email, function(error, info){
          callback(error, {email: email, info: info});
        });
      }else{
        return callback();
      }
    }
  }
];

var consumer = new eventr('myApp', connectionDetails, handlers);

consumer.on('error', function(error){ console.log('consumer error: ' + error); })
consumer.on('pause', function(partition){ console.log('consumer paused'); })
consumer.on('event', function(partition, event){ console.log('consumer found event: ' + JSON.stringify(event)  + ' on partition ' + partition); })
consumer.on('success', function(handler, results){ console.log('consumer success on handler: ' + handler); })
consumer.on('poll', function(partition){ console.log('consumer polling partition: ' + partition); })

consumer.work();
