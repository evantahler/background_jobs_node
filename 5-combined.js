var http       = require('http');
var nodemailer = require('nodemailer');
var NR         = require('node-resque');
var httpPort   = process.env.PORT || 8080;
var httpHost   = process.env.HOST || '127.0.0.1';

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

var jobs = {
  sendEmail: function(data, callback){
    var email = {
      from:    require('./.emailUsername'),
      to:      data.to,
      subject: data.subject,
      text:    data.text,
    };

    transporter.sendMail(email, function(error, info){
      callback(error, {email: email, info: info});
    });
  }
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

  queue.enqueue('emailQueue', "sendEmail", email, function(error){
    console.log('email :' + JSON.stringify(email));
    if(error){ console.log(error) }
    var response = {email: email};
    res.writeHead(200, {'Content-Type': 'application/json'});
    res.end(JSON.stringify(response, null, 2));
  });
};

var queue = new NR.queue({connection: connectionDetails}, jobs);
queue.connect(function(){
  http.createServer(server).listen(httpPort, httpHost);
  console.log('Server running at ' + httpHost + ':' + httpPort);
  console.log('send an email and message to /TO_ADDRESS/SUBJECT/YOUR_MESSAGE');
});

///////////////////
// RESQUE WORKER //
///////////////////

var worker = new NR.worker({connection: connectionDetails, queues: ['emailQueue']}, jobs);
worker.connect(function(){
  worker.workerCleanup();
  worker.start();
});

worker.on('start',           function(){ console.log("worker started"); });
worker.on('end',             function(){ console.log("worker ended"); });
worker.on('cleaning_worker', function(worker, pid){ console.log("cleaning old worker " + worker); });
worker.on('poll',            function(queue){ console.log("worker polling " + queue); });
worker.on('job',             function(queue, job){ console.log("working job " + queue + " " + JSON.stringify(job)); });
worker.on('reEnqueue',       function(queue, job, plugin){ console.log("reEnqueue job (" + plugin + ") " + queue + " " + JSON.stringify(job)); });
worker.on('success',         function(queue, job, result){ console.log("job success " + queue + " " + JSON.stringify(job) + " >> " + result); });
worker.on('failure',         function(queue, job, failure){ console.log("job failure " + queue + " " + JSON.stringify(job) + " >> " + failure); });
worker.on('error',           function(queue, job, error){ console.log("error " + queue + " " + JSON.stringify(job) + " >> " + error); });
worker.on('pause',           function(){ console.log("worker paused"); });
