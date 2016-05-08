var http       = require('http');
var nodemailer = require('nodemailer');
var Redis      = require('ioredis');
var httpPort   = process.env.PORT || 8080;
var httpHost   = process.env.HOST || '127.0.0.1';
var channel    = 'emailz';
var emails     = [];

var transporter = nodemailer.createTransport({
  service: 'gmail',
  auth: {
    user: require('./.emailUsername'),
    pass: require('./.emailPassword')
  }
});

var doServerStuff = function(){
  var publisher = Redis();

  var server = function(req, res){
    var urlParts = req.url.split('/');
    var email    = {
      to:      decodeURI(urlParts[1]),
      subject: decodeURI(urlParts[2]),
      text:    decodeURI(urlParts[3]),
    };

    publisher.publish(channel, JSON.stringify(email), function(){
      console.log('email :' + JSON.stringify(email));
      var response = {email: email};
      res.writeHead(200, {'Content-Type': 'application/json'});
      res.end(JSON.stringify(response, null, 2));
    });
  };

  http.createServer(server).listen(httpPort, httpHost);
  console.log('Server running at ' + httpHost + ':' + httpPort);
  console.log('send an email and message to /TO_ADDRESS/SUBJECT/YOUR_MESSAGE');
};

var doWorkerStuff = function(){
  var subscriber = Redis();

  subscriber.subscribe(channel);
  subscriber.on('message', function(channel, message){
    console.log('Message from Redis!');
    emails.push(JSON.parse(message));
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
      console.log('trying to send an email...');
      sendEmail(e.to, e.subject, e.text, function(error){
        if(error){
          emails.push(e); // try again
          console.log('failed sending email, trying again :(');
        }else{
          console.log('email sent!');
        }
        setTimeout(workerLoop, 1000);
      });
    }
  };

  workerLoop();
};

if(process.env.ROLE === 'server'){ doServerStuff(); }
if(process.env.ROLE === 'worker'){ doWorkerStuff(); }
