### Features that needs to be implemented
- [ ] Ensure client-server interaction works for 2..n clients with the snippets table.
- [ ] Server-side: Implement the algorithm for ordering operations.
- [ ] Server-side: Optimize the ordering of operations using LWW (Last Write Wins) to retain only the final state for each record.
- [ ] Server-side: Integrate with PostgreSQL to enable writing and reading data.
- [ ] Client-side: Apply received operations from the server in a single transaction.
- [ ] Client-server: Full state protocol should merge server materialized state with the client one
- [X] Client-side: Enable the client to translate any SQL statements (without WHERE) into tables and triggers used for collecting operations.
- [ ] Client and Server sides: Add logging.
