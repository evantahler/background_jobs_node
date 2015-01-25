var http          = require('http');
var nodemailer    = require('nodemailer');
var httpPort      = 8080 || process.env.port;

var transporter = nodemailer.createTransport({
    service: 'gmail',
    auth: { user: require('./.emailUsername'), pass: require('./.emailPassword') }
});

var sendEmail = function(req, callback){
  var urlParts     = req.url.split('/');
  var email        = {
    from:    require('./.emailUsername'),
    to:      urlParts[1],
    subject: urlParts[2],
    text:    urlParts[3],
  };
  transporter.sendMail(email, function(error, info){
    if(typeof callback === 'function'){ callback(error, email); }
  });
};

var server = function(req, res){
  var start = Date.now();
  var responseCode = 200;
  var response     = {};
  sendEmail(req);
  res.writeHead(responseCode, {'Content-Type': 'application/json'});
  res.end(JSON.stringify(response, null, 2));
};

http.createServer(server).listen(httpPort, '0.0.0.0');

console.log('Server running at http://0.0.0.0:' + httpPort);
console.log('send an email and message to /TO_ADDRESS/SUBJECT/YOUR_MESSAGE');