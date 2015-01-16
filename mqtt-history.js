#!/usr/bin/env node

var program = require('commander');
var redis = require('redis');
var mqtt = require('mqtt');

program.version('0.0.3')
  .option('-h, --redishostname [redis hostname]', 'redis DDB hostame or ip', null)
  .option('-p, --redisport [redis port]', 'redis DDB port number [6379]', 6379)
  .option('-m, --mqtthostname [mqtt hostname]', 'MQTT broker hostame or ip', null)
  .option('-q, --mqttport [mqtt port]', 'MQTT broker port number [1883]', 1883)
  .option('-t, --timestamp', 'Add timestamp to redis saved payload')
  .option('-r, --redisprefix [mqtt port]', 'Redis prefix', 'mqtt-history')
  .option('-s, --skipnullpayload', 'Do not save null payload')
  .option('-d, --debug', 'Debug mode')
  
	.parse(process.argv);


program.timestamp = program.timestamp || false;
program.skipnullpayload = program.skipnullpayload || false;
program.debug = program.debug || false;
//------------ DEBUG
function debug() {
  if (program.debug) {
    console.log.apply(this, arguments);
  }
}

var nop = function() {};

redisClient = redis.createClient(program.redisport, program.redishostname);
mqttClient = mqtt.createClient(program.mqtthostname, program.mqttport, {clientId: 'mqtt-history'});

mqttClient.on("connect", function() {
  debug ('MQTT connected');
  mqttClient.on('message', onMessage);
  mqttClient.subscribe('/#', {qos: 2}); // only once
});


function onMessage(topic, payload, packet) {
  if (program.skipnullpayload == true && !payload) {
    return;
  }
  // replace all / by :
  var redisTopic = topic.replace(/\//g, ':');
  if (redisTopic[0] == ':') {
    redisTopic = redisTopic.slice(1);
  }
  if (packet.retain == true) {
    //  try to avoid duplicate topic saved 
    //  get the last value stored for this topic
    //  ? this value is with timestamp
    //    yes : extract data
    //  ? compare payload to current payload
    //    same      :  abort
    //    different : save
    redisClient.lindex(program.redisprefix + ':' + redisTopic, 0, function(err, reply) {
      if (reply == null) {
        debug('reply null for ' + program.redisprefix + ':' + redisTopic);
        saveToRedis(redisTopic, payload);
        return;
      } 
      var value;
      try {
        value = JSON.parse(reply);
        if (value.timestamp && value.data) {
          if (value.data != payload) {
            saveToRedis(redisTopic, payload);
          } else {
            debug('topic ' + redisTopic + ' not saved 2');
          }
          return;
        }
      } catch (e) {
        // not a json compare payloads
        if (reply != payload) {
          saveToRedis(redisTopic, payload);
        } else {
          debug('topic ' + redisTopic + ' not saved 1');
        }
        return;
      }
    });
  } else {
    saveToRedis(redisTopic, payload);
  }
}

function saveToRedis(redisTopic, payload) {
  var history = {
    value : payload
  };
  if (program.timestamp == true) {
    history.timestamp = new Date();
  }
  redisClient.lpush(program.redisprefix + ':' + redisTopic, JSON.stringify(history));
  debug('lpush ' + program.redisprefix + ':' + redisTopic + ' : ' + JSON.stringify(history));
}
