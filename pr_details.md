# PR Title
docs(examples): Enhance db_queue README with performance analysis

# PR Summary
This PR significantly enhances the documentation for the `db_queue` example.

The updated README now includes:
- A detailed breakdown of the `jobs` table schema.
- Clear, step-by-step instructions for running the producer and consumer.
- An in-depth explanation of the different consumer algorithms (`GetNextJob`, `GetNextJobForUpdate`, `GetNextJobMin`, `GetNextJobMinForUpdate`), including the full SQL queries.
- Performance benchmark results for `GetNextJob` and `GetNextJobMin`, demonstrating that the `MIN(id)` approach is over 20x more performant in terms of both throughput and latency.
- An analysis of why `MIN(id)` is more efficient than `ORDER BY ... LIMIT 1` for this use case.

This improved documentation provides a much richer and more educational experience for users, clearly illustrating a critical performance optimization for high-throughput queueing systems.