'use strict';


var async = require('async');
var nohm  = require('nohm').Nohm;
var redis = require('redis').createClient();


// CONFIGURATION ///////////////////////////////////////////////////////////////


var USERS_AMOUNT  = 10;         // Amount of users
var TOPICS_AMOUNT = 100;        // Amount of topics
var POSTS_AMOUNT  = 1000;       // Amount of posts per topic to create
var RND_COUNTER   = Date.now(); // Initial state of COUNTER (used by rnd())


// INIT ////////////////////////////////////////////////////////////////////////


nohm.setClient(redis);


// HELPER FUNCTIONS ////////////////////////////////////////////////////////////


function rnd() {
  RND_COUNTER += 1;
  return RND_COUNTER;
}


function next(arr) {
  var idx = arr.__idx__;

  if (!arr[idx]) {
    idx = arr.__idx__ = 0;
  } else {
    arr.__idx__ += 1;
  }

  return arr[idx];
}


// MODELS //////////////////////////////////////////////////////////////////////


var User = nohm.model('User', {
  properties: {
    name: {
      type: 'string',
      unique: true,
      defaultValue: function () { return 'u' + rnd(); }
    },
    email: {
      type: 'string',
      unique: true,
      defaultValue: function () { return rnd() + '@nodeca.org'; }
    },
    password: {
      type: 'string',
      defaultValue: function () { return 'deadbeef'; }
    }
  }
});


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


function createUser(cb) {
  var user = new User();
  
  user.save(function (err) {
    console.log('USER: ' + this.id);
    cb(err, this);
  });
}


function createTopic(user, cb) {
  var topic = new Topic();

  // set relations
  topic.link(user, 'author');
  
  topic.save(function (err) {
    console.log('TOPIC: ' + this.id);
    cb(err, this);
  });
}


function createPost(user, topic, parent, cb) {
  var post = new Post();

  // set relations
  post.link(user, 'author');
  post.link(topic, 'topic');
  
  if (parent) {
    post.link(parent, 'parent');
  }
  
  post.save(function (err) {
    console.log('POST: ' + this.id);
    cb(err, this);
  });
}


// RUN FOR YOUR LIFE ///////////////////////////////////////////////////////////


async.waterfall([
  // create users
  function (cb) {
    var fns = [];

    while (fns.length < USERS_AMOUNT) {
      fns.push(function (cb) {
        createUser(cb);
      });
    }

    async.parallel(fns, cb);
  },
  //
  // create topics
  function (users, cb) {
    var fns = [];

    while (fns.length < TOPICS_AMOUNT) {
      fns.push(function (cb) {
        createTopic(next(users), cb);
      });
    }

    async.parallel(fns, function (err, topics) {
      cb(err, users, topics);
    });
  },
  //
  // create posts
  function (users, topics, cb) {
    var fns = [];

    topics.forEach(function (topic) {
      var subfns = [function (cb) {
        createPost(next(users), topic, null, cb);  
      }];

      while (subfns.length < POSTS_AMOUNT) {
        subfns.push(function (parent, cb) {
          createPost(next(users), topic, parent, cb);
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
