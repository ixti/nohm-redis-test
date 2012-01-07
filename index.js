'use strict';


var async = require('async');
var nohm  = require('nohm').Nohm;
var redis = require('redis').createClient();


// CONFIGURATION ///////////////////////////////////////////////////////////////


var TOPICS_AMOUNT = 100;        // Amount of topics
var POSTS_AMOUNT  = 1000;       // Amount of posts per topic to create


// INIT ////////////////////////////////////////////////////////////////////////


nohm.setClient(redis);


// HELPER FUNCTIONS ////////////////////////////////////////////////////////////


var RND_COUNTER = Date.now();
var DOT_COUNTER = [0, 0];


function rnd() {
  RND_COUNTER += 1;
  return RND_COUNTER;
}


function dot() {
  if (DOT_COUNTER[0] >= 100) {
    process.stdout.write('.');
    DOT_COUNTER[0] = 0;
    DOT_COUNTER[1] += 1;
  }

  if (DOT_COUNTER[1] >= 75) {
    process.stdout.write('\n');
    DOT_COUNTER[1] = 0;
  }

  DOT_COUNTER[0] += 1;
}


// MODELS //////////////////////////////////////////////////////////////////////


var Topic = nohm.model('Topic', {
  properties: {
    title: {
      type: 'string',
      unique: true,
      defaultValue: function () { return 'Topic #' + rnd(); }
    }
  }
});


var Post = nohm.model('Post', {
  properties: {
    title: {
      type: 'string',
      unique: true,
      defaultValue: function () { return 'Post #' + rnd(); }
    }
  }
});


// FACTORIES ///////////////////////////////////////////////////////////////////


function createTopic(cb) {
  var topic = new Topic();

  topic.save(function (err) {
    dot();
    cb(err, this);
  });
}


function createPost(topic, parent, cb) {
  var post = new Post();

  // set relations
  post.link(topic, 'topic');
  if (parent) {
    post.link(parent, 'parent');
  }
  
  post.save(function (err) {
    dot();
    cb(err, this);
  });
}


// RUN FOR YOUR LIFE ///////////////////////////////////////////////////////////


async.waterfall([
  // create topics
  function (cb) {
    var fns = [];

    while (fns.length < TOPICS_AMOUNT) {
      fns.push(function (cb) {
        createTopic(cb);
      });
    }

    async.parallel(fns, function (err, topics) {
      cb(err, topics);
    });
  },
  //
  // create posts
  function (topics, cb) {
    var fns = [];

    topics.forEach(function (topic) {
      var subfns = [function (cb) {
        createPost(topic, null, cb);  
      }];

      while (subfns.length < POSTS_AMOUNT) {
        subfns.push(function (parent, cb) {
          createPost(topic, parent, cb);
        });
      }

      fns.push(function (cb) {
        async.waterfall(subfns, function (err) {
          // avoid passing results. pass error only.
          cb(err);
        });
      });
    });

    async.parallel(fns, cb);
  },
], function (err) {
  if (err) {
    console.error(err);
    console.error('FAIL');
    process.exit(1);
  } else {
    console.log('DONE');
    process.exit(0);
  }
});
