#!/usr/bin/env node

var program = require('commander');
var redis = require('redis');
var mqtt = require('mqtt');

program.version('0.0.1')
  .option('-h, --redishostname [redis hostname]', 'redis DDB hostame or ip', null)
  .option('-p, --redisport [redis port]', 'redis DDB port number [6379]', 6379)
  .option('-m, --mqtthostname [mqtt hostname]', 'MQTT broker hostame or ip', null)
  .option('-q, --mqttport [mqtt port]', 'MQTT broker port number [1883]', 1883)
	.parse(process.argv);

//------------ DEBUG
var debug_on = false;
function debug() {
  if (debug_on) console.log.apply(this, arguments);
}
var nop = function() {};
console.log(program.mqtthostname);

redisClient = redis.createClient(program.redisport, program.redishostname);
mqttClient = mqtt.createClient(program.mqtthostname, program.mqttport);
mqttClient.on("connect", function() {
  debug ('MQTT connected');
  mqttClient.on('message', function(topic, payload) {  
    // replace all / by :
    var redisTopic = topic.replace(/\//g, ':');
    if (redisTopic[0] == ':') {
      redisTopic = redisTopic.slice(1);
    }
    debug('lpush ' + redisTopic + ' to redis');
    redisClient.lpush('MQTTHistory:' + redisTopic, payload);
  });
  mqttClient.subscribe('/#');
});
