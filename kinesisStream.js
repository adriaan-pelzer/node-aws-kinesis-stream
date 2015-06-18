var aws = require ( 'aws-sdk' );
var R = require ( 'ramda' );
var hl = require ( 'highland' );

var processResult = function ( process, push ) {
    var pushFuncs = {
        error: push,
        success: R.lPartial ( push, null ),
        successList: R.forEach ( R.lPartial ( push, null ) )
    };

    return function ( error, result ) {
        if ( error ) {
            pushFuncs.error ( error );
            pushFuncs.success ( hl.nil );
        } else {
            process ( pushFuncs, result );
        }
    };
};

var wrapKinesisFunction = function ( kinesis, func, parms ) {
    return function ( push, next ) {
        kinesis[func] ( parms, processResult ( function ( pushFuncs, result ) {
            pushFuncs.success ( result );
            pushFuncs.success ( hl.nil );
        }, push ) );
    };
};

var getNextIteration = function ( kinesis, nextShardIterator, timeout ) {
    return hl ( function ( push, next ) {
        kinesis.getRecords ( { ShardIterator: nextShardIterator, Limit: 1000 }, processResult ( function ( pushFuncs, result ) {
            if ( R.type ( result.Records ) !== 'Array' ) {
                pushFuncs.error ( 'Results is not an array' );
            } else {
                if ( R.isEmpty ( result.Records ) ) {
                    setTimeout ( function () {
                        /* What to do if result.NextShardIterator is undefined? */
                        next ( getNextIteration ( kinesis, result.NextShardIterator, R.min ( [ timeout + 100, 1000 ] ) ) );
                    }, timeout );
                } else {
                    pushFuncs.successList ( result.Records );
                    setTimeout ( function () {
                        next ( getNextIteration ( kinesis, result.NextShardIterator, 100 ) );
                    }, 100 );
                }
            }
        }, push ) );
    } );
};

module.exports = R.curry ( function ( awsConfig, streamName, ShardIteratorType ) {
    var kinesis = new aws.Kinesis ( ( R.type ( awsConfig ) === 'string' ) ? { region: awsConfig } : awsConfig );
    var kinesisConfig = R.mixin ( R.createMapEntry ( 'StreamName', streamName ), R.createMapEntry ( 'ShardIteratorType', ShardIteratorType ) );

    return hl ( wrapKinesisFunction ( kinesis, 'describeStream', R.omit ( [ 'ShardIteratorType' ], kinesisConfig ) ) )
        .pluck ( 'StreamDescription' )
        .pluck ( 'Shards' )
        .map ( R.pluck ( 'ShardId' ) )
        .flatMap ( function ( shardIdArray ) {
            return hl ( shardIdArray )
                .map ( R.createMapEntry ( 'ShardId' ) )
                .map ( R.mixin ( kinesisConfig ) )
                .map ( function ( shardConfig ) {
                    return hl ( wrapKinesisFunction ( kinesis, 'getShardIterator', shardConfig ) ).flatMap ( function ( result ) {
                        return getNextIteration ( kinesis, result.ShardIterator, 0 );
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
