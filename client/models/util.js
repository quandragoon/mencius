var fs = require('fs');
var hbs = require('hbs');

exports.registerPartial = function(page) {
  var contentDir = __dirname.substring(0, __dirname.lastIndexOf('/'));
  var contentFile = contentDir + '/views/partials/' + page + '.html';
  var contents = fs.readFileSync(contentFile, 'utf8');
  hbs.registerPartial('content', contents);
}