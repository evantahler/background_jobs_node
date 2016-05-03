var cluster    = require('cluster');
var http       = require('http');
var nodemailer = require('nodemailer');
var httpPort   = process.env.PORT || 8080;
var httpHost   = process.env.HOST || '127.0.0.1';
var children   = {};
var emails     = [];

var transporter = nodemailer.createTransport({
  service: 'gmail',
  auth: {
    user: require('./.emailUsername'),
    pass: require('./.emailPassword')
  }
});

var log = function(p, msg){
  var name, pid;
  if(p.name !== undefined){
    name = p.name;
    pid  = p.process.pid;
  }else{
    name = 'master';
    pid = process.pid;
  }
  console.log('[' + name + ' @ ' + pid + '] ' + msg);
};

var doMasterStuff = function(){
  log('master', 'started master');

  var masterLoop = function(){
    checkOnWebServer();
    checkOnEmailWorker();
  };

  var checkOnWebServer = function(){
    if(children.server === undefined){
      log('master', 'starting web server');
      children.server = cluster.fork({ROLE: 'server'});
      children.server.name = 'web server';
      children.server.on('online',    function(){ log(children.server, 'ready on port ' + httpPort); });
      children.server.on('exit',      function(){
        log(children.server, 'died :(');
        delete children.server;
      });
      children.server.on('message',   function(message){
        log(children.server, 'got an email to send from the webserver: ' + JSON.stringify(message));
        children.worker.send(message);
      });
    }
  };

  var checkOnEmailWorker = function(){
    if(children.worker === undefined){
      log('master', 'starting email worker');
      children.worker = cluster.fork({ROLE: 'worker'});
      children.worker.name = 'email worker';
      children.worker.on('online',    function(){ log(children.worker, 'ready!'); });
      children.worker.on('exit',      function(){
        log(children.worker, 'died :(');
        delete children.worker;
      });
      children.worker.on('message',   function(message){
        log(children.worker, JSON.stringify(message));
      });
    }
  };

  setInterval(masterLoop, 1000);
};

var doServerStuff = function(){
  var server = function(req, res){
    var urlParts = req.url.split('/');
    var email    = {
      to:      decodeURI(urlParts[1]),
      subject: decodeURI(urlParts[2]),
      text:    decodeURI(urlParts[3]),
    };

    var response = {email: email};
    res.writeHead(200, {'Content-Type': 'application/json'});
    res.end(JSON.stringify(response, null, 2));

    process.send(email);
  };

  http.createServer(server).listen(httpPort, httpHost);
};

var doWorkerStuff = function(){
  process.on('message', function(message){
    emails.push(message);
  });

  var sendEmail = function(to, subject, text, callback){
    var email = {
      from:    require('./.emailUsername'),
      to:      to,
      subject: subject,
      text:    text,
    };

    transporter.sendMail(email, function(error, info){
      callback(error, email);
    });
  };

  var workerLoop = function(){
    if(emails.length === 0){
      setTimeout(workerLoop, 1000);
    }else{
      var e = emails.shift();
      process.send({msg: 'trying to send an email...'});
      sendEmail(e.to, e.subject, e.text, function(error){
        if(error){
          emails.push(e); // try again
          process.send({msg: 'failed sending email, trying again :('});
        }else{
          process.send({msg: 'email sent!'});
        }
        setTimeout(workerLoop, 1000);
      });
    }
  };

  workerLoop();
};

if(cluster.isMaster){
  doMasterStuff();
}else{
  if(process.env.ROLE === 'server'){ doServerStuff(); }
  if(process.env.ROLE === 'worker'){ doWorkerStuff(); }
}
