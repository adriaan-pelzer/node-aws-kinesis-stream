var K = require ( '../kinesisStream.js' )( {
    debug: true,
    StreamName: 'OmnitureStatsStream',
    ShardIteratorType: 'LATEST',
    region: 'eu-west-1'
} );

var noop = function () {};

K.each ( noop );
