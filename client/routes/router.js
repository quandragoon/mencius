var express = require('express');
var util = require('../models/util');

/* GET home page. */
exports.index = function(req, res) {
  res.render('index');
}

exports.game2048 = function(req, res) {
  util.registerPartial('2048');
  util.registerHeader('2048');
  res.render('base', { title: 'Express' });
}

exports.chess = function(req, res) {
  util.registerPartial('chess');
  util.registerHeader('chess');
  res.render('base', { title: 'Express' });
}