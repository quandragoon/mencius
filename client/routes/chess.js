var express = require('express');
var util = require('../models/util');

var router = express.Router();

/* GET home page. */
router.get('/', function(req, res) {
  util.registerPartial('chess');
  res.render('base', { title: 'Express' });
});

module.exports = router;
