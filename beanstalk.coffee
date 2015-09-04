_ = require 'lodash'
bs = require 'nodestalker'
Promise = require 'bluebird'
log = require 'loga'

RUNNER_ERROR_DELAY_MS = 1000
DEFAULT_PRIORITY = 10
DEFAULT_TIME_TO_RUN_SECONDS = 100000 # sufficiently large for most tasks

JOB_TYPES =
  DEFAULT: 'mobile-api__default' # restricted character set

watchJobs = (runner, type, handlerFn) ->
  fetchJobs = ->
    new Promise (resolve, reject) ->
      runner.reserve().onSuccess (job) ->
        data = JSON.parse job.data
        new Promise (resolve) -> resolve handlerFn(data)
        .then ->
          runner.deleteJob(job.id)
          .onSuccess resolve
          .onError reject
        .catch reject
      .onError reject
    .catch (err) ->
      log.error err
      new Promise (resolve) ->
        setTimeout resolve, RUNNER_ERROR_DELAY_MS
    .then fetchJobs

  runner.watch(type).onSuccess -> fetchJobs()
  .onError log.error

class Beanstalk
  JOB_TYPES: JOB_TYPES
  constructor: ->
    @queuer = bs.Client '127.0.0.1:11300'

  listen: =>
    _.map JOB_TYPES, (type) =>
      runner = bs.Client '127.0.0.1:11300'

      watchJobs runner, type, (job) =>
        switch type
          when JOB_TYPES.DEFAULT
            @runDefaultJob job
          else
            throw new Error "Unknown job type #{type}"

  runDefaultJob: ({x}) ->
    console.log 'x', x

  createJob: ({job, priority, timeToRunSeconds, delaySeconds, type}) =>
    unless type? and _.includes _.values(JOB_TYPES), type
      throw new Error 'Must specify a valid job type'

    priority ?= DEFAULT_PRIORITY
    timeToRunSeconds ?= DEFAULT_TIME_TO_RUN_SECONDS
    delaySeconds ?= 0
    payload = JSON.stringify job

    new Promise (resolve, reject) =>
      @queuer.use(type).onSuccess =>
        @queuer.put(payload, priority, delaySeconds, timeToRunSeconds)
        .onSuccess resolve
        .onError reject
      .onError reject


beanstalk = new Beanstalk()
beanstalk.listen()

beanstalk.createJob {job: {x: 'y1'}, delaySeconds: 3, type: JOB_TYPES.DEFAULT}
.then -> console.log 'created1'
beanstalk.createJob {job: {x: 'y2'}, delaySeconds: 1, type: JOB_TYPES.DEFAULT}
.then -> console.log 'created2'
