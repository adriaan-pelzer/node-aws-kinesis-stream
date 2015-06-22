var aws = require ( 'aws-sdk' );
var R = require ( 'ramda' );
var hl = require ( 'highland' );

var timeouts = [ 0, 2000 ];
var debug = false;

var log = function ( message ) {
    var inspect;

    if ( debug ) {
        inspect = require ( 'eyes' ).inspector ( { maxLength: 0 } );

        ( {
            'Array': inspect,
            'Object': inspect
        }[ R.type ( message ) ] || console.log )( message );
    }
};

var wrapKinesisFunction = R.curry ( function ( kinesis, func, parms ) {
    log ( 'wrapKinesisFunction' );

    return hl.wrapCallback ( function ( parms, callBack ) {
        kinesis[func] ( parms, callBack );
    } )( parms );
} );

var getNextIteration = function ( kinesis, nextShardIterator, timeout ) {
    log ( 'getNextIteration' );
    log ( 'timeout: ' + timeout );

    return wrapKinesisFunction ( kinesis, 'getRecords', { ShardIterator: nextShardIterator, Limit: 10000 } )
        .errors ( function ( error, push ) {
            log ( 'error' );

            if ( error.code !== 'ProvisionedThroughputExceededException' ) {
                log ( 'It\'s a real error' );
                return push ( error );
            }

            log ( 'It\'s a throughput error' );

            return push ( null, { NextShardIterator: nextShardIterator, Records: [] } );
        } )
        .flatMap ( function ( result ) {
            log ( 'MillisBehindLatest: ' + result.MillisBehindLatest );

            return hl ( function ( push, next ) {
                if ( R.isEmpty ( result.Records ) ) {
                    log ( 'No Records' );
                    return setTimeout ( function () {
                        next ( getNextIteration ( kinesis, result.NextShardIterator, R.min ( [ timeout + R.min ( timeouts ), R.max ( timeouts ) ] ) ) );
                    }, timeout );
                }

                log ( 'Has Records' );
                log ( 'Number of Records: ' + R.length ( result.Records ) );

                R.forEach ( R.curry ( push )( null, R.__ ), result.Records );

                setTimeout ( function () {
                    next ( getNextIteration ( kinesis, result.NextShardIterator, R.max ( [ timeout - R.min ( timeouts ), R.min ( timeouts ) ] ) ) );
                }, R.min ( timeouts ) );
            } );
        } );
};

module.exports = R.curry ( function ( awsConfig ) {
    var kinesis = new aws.Kinesis ( R.pick ( [ 'accessKeyId', 'secretAccessKey', 'region' ], awsConfig ) );
    var kinesisConfig = R.pick ( [ 'StreamName', 'ShardIteratorType' ], awsConfig );
    
    debug = awsConfig.debug || false;

    return wrapKinesisFunction ( kinesis, 'describeStream', R.omit ( [ 'ShardIteratorType' ], kinesisConfig ) )
        .pluck ( 'StreamDescription' )
        .pluck ( 'Shards' )
        .map ( R.pluck ( 'ShardId' ) )
        .flatMap ( function ( shardIdArray ) {
            return hl ( shardIdArray )
                .map ( R.createMapEntry ( 'ShardId' ) )
                .map ( R.merge ( kinesisConfig ) )
                .map ( function ( shardConfig ) {
                    return wrapKinesisFunction ( kinesis, 'getShardIterator', shardConfig )
                        .flatMap ( function ( result ) {
                            return getNextIteration ( kinesis, result.ShardIterator, R.min ( timeouts ) );
                        } );
                } )
                .parallel ( R.length ( shardIdArray ) );
        } )
        .map ( function ( record ) {
            return record.Data.toString ();
        } )
        .flatMap ( function ( record ) {
            return hl.wrapCallback ( function ( record, callBack ) {
                try {
                    return callBack ( null, JSON.parse ( record ) );
                } catch ( error ) {
                    return callBack ( error );
                }
            } )( record );
        } );
} );
