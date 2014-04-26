var express = require('express');
var http = require('http');
var path = require('path');
var favicon = require('static-favicon');
var Consolidate = require('consolidate');
var bodyParser = require('body-parser');
var logger = require('morgan');
var hbs = require('hbs');

var routes = require('./routes/router');

var app = express()
  , server = http.createServer(app).listen(8080, function(){
      console.log('Express server listening on port ' + 8080);
    })
  , io = require('socket.io').listen(server);

// view engine setup
app.set('views', __dirname + '/views');
app.set('view engine', 'html');
app.engine('html', require('hbs').__express);
app.set('view options', {layout: false});

// hbs.registerPartials(__dirname + '/views/partials');
app.use(express.static(path.join(__dirname, 'public')));
app.use(bodyParser());
// end view engine setup

app.use(favicon());
app.use(logger('dev'));
app.use(express.static(path.join(__dirname, 'public')));

app.get('/', routes.index);
app.get('/chess', routes.chess);
app.get('/2048', routes.game2048);

/// catch 404 and forwarding to error handler
app.use(function(req, res, next) {
    var err = new Error('Not Found');
    err.status = 404;
    next(err);
});

/// error handlers

// development error handler
// will print stacktrace
if (app.get('env') === 'development') {
    app.use(function(err, req, res, next) {
        res.status(err.status || 500);
        res.render('error', {
            message: err.message,
            error: err
        });
    });
}

// production error handler
// no stacktraces leaked to user
app.use(function(err, req, res, next) {
    res.status(err.status || 500);
    res.render('error', {
        message: err.message,
        error: {}
    });
});

// socket stuff
io.sockets.on('connection', function (socket) {
  socket.emit('news', { hello: 'world' });
  socket.on('my other event', function (data) {
    console.log(data);
  });
});

module.exports = app;
