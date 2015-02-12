# node-aws-kinesis-stream

usage:
npm install node-aws-kinesis-stream

var kinesis = require ( 'node-aws-kinesis-stream' )( 'eu-west-1' );

kinesis ( streamName ) = a highland stream, and will emit all new objects added to the kinesis stream.
