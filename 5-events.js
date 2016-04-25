var nodemailer = require('nodemailer');
var httpPort   = process.env.PORT || 8080;
var httpHost   = process.env.HOST || '127.0.0.1';
var redis      = require('ioredis');
var async      = require('async');

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

var eventr = function(name, connectionDetails){
  this.redis = new redis(connectionDetails);
  this.name = name;
  this.prefix = 'eventr:';
  this.partitionSizeInSeconds = 60;
  this.partitionDriftAllotmentSeconds = 30;

  var luaLines = [];
  // get the counter for this named consumer
  luaLines.push('local counter = 0');
  luaLines.push('if redis.call("HEXISTS", "' + this.prefix + 'counters", KEYS[1]) == 1 then');
  luaLines.push('  local counter = redis.call("HGET", "' + this.prefix + 'counters", KEYS[1])');
  luaLines.push('end');
  // if the partition exists, get the data from that key
  // otherwise + the partition and end
  luaLines.push('local partition = "' + this.prefix + 'partitions:" .. KEYS[2]');
  luaLines.push('if reids.call("EXISTS", partition) == 0 then');
  luaLines.push('  retun nil');
  luaLines.push('else ');
  luaLines.push('  local event = reids.call("LRANGE", partition, counter, counter)');
  luaLines.push('  reids.call("HSET", "' + this.prefix + ':counters", (counter + 1))');
  luaLines.push('  return event');
  luaLines.push('end');


  this.redis.defineCommand('getAndIncr', {
    numberOfKeys: 2, lua: luaLines.join('\r\n')
  });
};

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
  self.redis.zrange(self.prefix + 'partitions', 0, 0, callback);
}

eventr.prototype.ensurePartition = function(partition, callback){
  var self = this;
  self.redis.zadd(self.prefix + 'partitions', partition, partition, callback);
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
          return next();
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
    self.redis.getAndIncr(self.name, partition, function(error, e){
      if(error){ return next(error); }
      if(e){ event = e; }
      return next();
    });
  });

  // if we got no event, and we are sufffiecently in the future, try a new partition
  jobs.push(function(next){
    if(event){ return next(); }
    if(!partition){ return next(); }
    if( self.nowPartition() > partition ){
      partition++; //TODO: we can speed this up by checking the ZSETS of partition names
      self.redis.hset(self.prefix + 'client_partitions', self.name, partition, next);
    }
  });

  async.series(jobs, function(error){
    if(error){ return callback(error); }
    return callback(null, partition, event);
  });
};

//////////
// TEST //
//////////

e = new eventr('testr');
e.write({name: 'user_created'}, function(error){
  if(error){ console.log(error); }
  console.log("wrote...");
  e.next(function(error, event){
    console.log(error)
    console.log(event)
  });
});
