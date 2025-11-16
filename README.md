# carabiner

carabiner is a library to let you build durable background tasks that withstand device restarts, task crashes, and long-running jobs. It's built for Python and Postgres without any additional deploy time requirements.

## Philosophy

Background jobs in webapps are so frequently used that they should really be a primitive of your fullstack library: database, backend, frontend, _and_ background jobs. Otherwise you're stuck in a situation where users either have to always make blocking requests to an API or you spin up ephemeral background jobs that will be killed during re-deployments or an accidental docker crash.

After trying most of the ecosystem in the last 3 years, I believe background jobs should provide a few key features:

- Easy to write control flow in normal Python
- Control flow shouldn't be forced into a DAG definition; it should be regular control flow
- Should be both very simple to test locally and very simple to deploy remotely
- Reasonable default configurations to scale to a reasonable request volume without performance tuning

Nothing on the market provides this balance - `carabiner` aims to try. We don't expect ourselves to reach best in class functionality for load performance. Instead we intend for this to scale _most_ applications well past product market fit.

## Other options

There is no shortage of robust background queues in Python, including ones that scale to millions of requests a second:

1. Temporal.io
2. Celery/RabbitMQ

Almost all of these require a dedicated task broker that you host alongside your app. This usually isn't a huge deal during POCs, but they all have a ton of knobs and dials so you can performance tune it to your own environment. It's also yet another thing in which to build a competency. Cloud hosting of most of these are billed per-event and can get very expensive depending on how you orchestrate your jobs.
