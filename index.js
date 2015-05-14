var tmp = require('tmp'),
    http = require('http'),
    fs = require('fs'),
    async = require('async'),
    _ = require('underscore'),
    parse = require('csv-parse'),
    utf8 = require('to-utf-8'),
    mongoose = require('mongoose'),
    Schema = mongoose.Schema;

async.waterfall([
    function (callback) {
        var paths = {};

        var path = tmp.fileSync({prefix: 'stops-', postfix: '.csv'});
        paths.stops = path.name;

        console.log('Retrieving stops.csv...');
        http.get('http://193.69.180.119:8080/tabledump/stops2.csv', function (response) {
            var stream = response.pipe(fs.createWriteStream(path.name));
            stream.on('error', function (err) {
                callback(err);
            }).on('finish', function (err) {
                callback(null, paths);
            });
        });
    },
    function (paths, callback) {
        var path = tmp.fileSync({prefix: 'lines-', postfix: '.csv'});
        paths.lines = path.name;

        console.log('Retrieving lines.csv...');
        http.get('http://193.69.180.119:8080/tabledump/lines.csv', function (response) {
            var stream = response.pipe(fs.createWriteStream(path.name));
            stream.on('error', function (err) {
                callback(err);
            }).on('finish', function (err) {
                callback(null, paths);
            });
        })
    },
    function (paths, callback) {
        var path = tmp.fileSync({prefix: 'lines-stops-', postfix: '.csv'});
        paths.linesStops = path.name;

        console.log('Retrieving lines_stops.csv...');
        http.get('http://193.69.180.119:8080/tabledump/lines_stops.csv', function (response) {
            var stream = response.pipe(fs.createWriteStream(path.name));
            stream.on('error', function (err) {
                callback(err);
            }).on('finish', function (err) {
                callback(null, paths);
            });
        });
    }, function (paths, callback) {
        console.log('Connecting to mongo database...');
        mongoose.connect('mongodb://127.0.0.1:3001/meteor', function (err) {
            if (err) {
                callback(err);
            } else {
                console.log('Connected to mongo database');
                callback(null, paths);
            }
        });
    }, function (paths, callback) {
        console.log('Dropping collection stops...');
        var stops = mongoose.connection.db.collections['stops'];

        if (stops) {
            stops.drop(function (err, result) {
                if (err) {
                    console.log(err);
                    callback(err);
                } else {
                    callback(null, paths);
                }
            });
        } else {
            callback(null, paths);
        }
    }, function (paths, callback) {
        console.log('Dropping collection lines...');
        var lines = mongoose.connection.db.collections['lines'];

        if (lines) {
            lines.drop(function (err, result) {
                if (err) {
                    console.log(err);
                    callback(err);
                } else {
                    callback(null, paths);
                }
            });
        } else {
            callback(null, paths);
        }
    }, function (paths, callback) {
        console.log('Dropping collection lines_stops...');
        var linesStops = mongoose.connection.db.collections['linesstops'];

        if (linesStops) {
            linesStops.drop(function (err, result) {
                if (err) {
                    console.log(err);
                    callback(err);
                } else {
                    callback(null, paths);
                }
            });
        } else {
            callback(null, paths);
        }
    }, function (paths, callback) {
        var Post = mongoose.model('Post', new Schema({
            stopId: Number,
            name: String,
            x: Number,
            y: Number
        }));

        var posts=[];

        var parser = parse({delimiter: ';'}, function (err, data) {
            _.each(data, function (value) {
                var post = new Post({
                    stopId: value[0],
                    name: value[1],
                    x: value[5],
                    y: value[6]
                });

                //console.log(post);
                posts.push(post);
            });

            console.log(posts.length);
            Post.collection.insert(posts, function(err, docs) {
                if (err) {
                    callback(err);
                } else {
                    console.log('Finished inserting ' + docs.length + ' stops');
                    callback(null, paths);
                }
            });
        });

        console.log('Parsing stops.csv...');
        fs.createReadStream(paths.stops).pipe(utf8()).pipe(parser);
    }, function (paths, callback) {
        var Line = mongoose.model('Line', new Schema({
            lineId: Number,
            name: String,
            type: Number
        }));

        var lines = [];

        var parser = parse({delimiter: ';'}, function (err, data) {
            _.each(data, function (value) {
                var line = new Line({
                    lineId: value[0],
                    name: value[1].trim(),
                    type: value[2]
                });

                //console.log(line);
                lines.push(line);
            });

            Line.collection.insert(lines, function(err, docs) {
                if (err) {
                    callback(err);
                } else {
                    console.log('Finished inserting ' + docs.length + ' lines');
                    callback(null, paths);
                }
            });
        });

        console.log('Parsing lines.csv...');
        fs.createReadStream(paths.lines).pipe(utf8()).pipe(parser);
    }, function (paths, callback) {
        var LineStop = mongoose.model('LinesStop', new Schema({
            lineId: Number,
            stopId: Number
        }));

        var lineStops = [];

        var parser = parse({delimiter: ';'}, function (err, data) {
            _.each(data, function (value) {
                var lineStop = new LineStop({
                    lineId: value[0],
                    stopId: value[1]
                });

                //console.log(lineStop);
                lineStops.push(lineStop);
            });
        });

        LineStop.collection.insert(lineStops, function(err, docs) {
            if (err) {
                callback(err);
            } else {
                console.log('Finished inserting ' + docs.length + ' line_stops');
                callback(null);
            }
        });

        console.log('Parsing lines_stops.csv...');
        fs.createReadStream(paths.linesStops).pipe(utf8()).pipe(parser);
    }, function(callback) {
        mongoose.connection.close();
    }
]);